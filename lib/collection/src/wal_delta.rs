use std::fmt::Debug;
use std::sync::Arc;

use parking_lot::Mutex as ParkingMutex;
use thiserror::Error;
use tokio::sync::Mutex;

use crate::operations::OperationWithClockTag;
use crate::shards::local_shard::clock_map::{ClockMap, RecoveryPoint};
use crate::wal::SerdeWal;

pub type LockedWal = Arc<ParkingMutex<SerdeWal<OperationWithClockTag>>>;

/// A WAL that is recoverable, with operations having clock tags and a corresponding clock map.
pub struct RecoverableWal {
    pub(super) wal: LockedWal,
    /// Map of all last seen clocks for each peer and clock ID.
    pub(super) last_clocks: Arc<Mutex<ClockMap>>,
}

impl RecoverableWal {
    pub fn from(wal: LockedWal, last_clocks: Arc<Mutex<ClockMap>>) -> Self {
        Self { wal, last_clocks }
    }

    /// Write a record to the WAL but does guarantee durability.
    pub async fn lock_and_write(
        &self,
        operation: &mut OperationWithClockTag,
    ) -> crate::wal::Result<u64> {
        // Update last seen clock map and correct clock tag if necessary
        if let Some(clock_tag) = &mut operation.clock_tag {
            // TODO:
            //
            // Temporarily accept *all* operations, even if their `clock_tag` is older than
            // current clock tracked by the clock map

            // TODO: do not manually advance here!
            let _operation_accepted = self
                .last_clocks
                .lock()
                .await
                .advance_clock_and_correct_tag(clock_tag);

            // if !operation_accepted {
            //     return Ok(UpdateResult {
            //         operation_id: None,
            //         status: UpdateStatus::Acknowledged,
            //         clock_tag: Some(*clock_tag),
            //     });
            // }
        }

        // Write operation to WAL
        self.wal.lock().write(operation)
    }

    /// Get a recovery point for this WAL.
    pub async fn recovery_point(&self) -> RecoveryPoint {
        self.last_clocks.lock().await.to_recovery_point()
    }

    pub async fn resolve_wal_delta(
        &self,
        recovery_point: RecoveryPoint,
    ) -> Result<u64, WalDeltaError> {
        resolve_wal_delta(
            recovery_point,
            self.wal.clone(),
            &self.recovery_point().await,
        )
    }

    /// Append records to this WAL from `other`, starting at operation `append_from` in `other`.
    #[cfg(test)]
    pub async fn append_from(&self, other: &Self, append_from: u64) -> crate::wal::Result<()> {
        let mut operations = other
            .wal
            .lock()
            .read(append_from)
            .map(|(_, op)| op)
            .collect::<Vec<_>>();
        for update in operations.iter_mut() {
            self.lock_and_write(update).await?;
        }
        Ok(())
    }
}

/// Resolve the WAL delta for the given `recovery_point`
///
/// A `local_wal` and `local_last_seen` are required to resolve the delta. These should be from the
/// node being the source of recovery, likely the current one. The `local_wal` is used to resolve
/// the diff. The `local_last_seen` is used to extend the given recovery point with clocks the
/// failed node does not know about.
///
/// The delta can be sent over to the node which the recovery point is from, to restore its
/// WAL making it consistent with the current shard.
///
/// On success, a WAL record number from which the delta is resolved in the given WAL is returned.
/// If a WAL delta could not be resolved, an error is returned describing the failure.
fn resolve_wal_delta(
    mut recovery_point: RecoveryPoint,
    local_wal: LockedWal,
    local_recovery_point: &RecoveryPoint,
) -> Result<u64, WalDeltaError> {
    // If the recovery point has clocks our current node does not know about
    // we're missing essential operations and cannot resolve a WAL delta
    if recovery_point.has_clocks_not_in(local_recovery_point) {
        return Err(WalDeltaError::UnknownClocks);
    }

    // If our current node has any lower clock than the recovery point specifies,
    // we're missing essential operations and cannot resolve a WAL delta
    if recovery_point.has_any_higher(local_recovery_point) {
        return Err(WalDeltaError::HigherThanCurrent);
    }

    // Extend clock map with missing clocks this node know about
    // Ensure the recovering node gets records for a clock it might not have seen yet
    recovery_point.extend_with_missing_clocks(local_recovery_point);

    // If recovery point is empty, we cannot do a diff transfer
    if recovery_point.is_empty() {
        return Err(WalDeltaError::Empty);
    }

    // Remove clocks that are equal to this node, we don't have to transfer records for them
    // TODO: do we want to remove higher clocks too, as the recovery node already has all data?
    recovery_point.remove_equal_clocks(local_recovery_point);

    // TODO: check truncated clock values or each clock we have:
    // TODO: - if truncated is higher, we cannot resolve diff

    // Scroll back over the WAL and find a record that covered all clocks
    // Drain satisfied clocks from the recovery point until we have nothing left
    log::trace!("Resolving WAL delta for: {recovery_point}");
    let delta_from = local_wal
        .lock()
        .read_from_last(true)
        .filter_map(|(op_num, update)| update.clock_tag.map(|clock_tag| (op_num, clock_tag)))
        // Keep scrolling until we have no clocks left
        .find(|(_, clock_tag)| {
            recovery_point.remove_equal_or_lower(*clock_tag);
            recovery_point.is_empty()
        })
        .map(|(op_num, _)| op_num);

    delta_from.ok_or(WalDeltaError::NotFound)
}

#[derive(Error, Debug, Clone)]
#[error("cannot resolve WAL delta: {0}")]
pub enum WalDeltaError {
    #[error("recovery point has no clocks to resolve delta for")]
    Empty,
    #[error("recovery point requests clocks this WAL does not know about")]
    UnknownClocks,
    #[error("recovery point requests higher clocks this WAL has")]
    HigherThanCurrent,
    #[error("recovery point requests clocks truncated from this WAL")]
    Truncated,
    #[error("cannot find slice of WAL operations that satisfies the recovery point")]
    NotFound,
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::sync::Arc;

    use parking_lot::Mutex as ParkingMutex;
    use tempfile::{Builder, TempDir};
    use wal::WalOptions;

    use super::*;
    use crate::operations::point_ops::{
        PointInsertOperationsInternal, PointOperations, PointStruct,
    };
    use crate::operations::{ClockTag, CollectionUpdateOperations, OperationWithClockTag};
    use crate::shards::local_shard::clock_map::{ClockMap, RecoveryPoint};
    use crate::shards::replica_set::clock_set::ClockSet;
    use crate::wal::SerdeWal;

    fn fixture_empty_wal() -> (RecoverableWal, TempDir) {
        let dir = Builder::new().prefix("wal_test").tempdir().unwrap();
        let options = WalOptions {
            segment_capacity: 1024 * 1024,
            segment_queue_len: 0,
        };
        let wal = SerdeWal::new(dir.path().to_str().unwrap(), options).unwrap();
        (
            RecoverableWal::from(
                Arc::new(ParkingMutex::new(wal)),
                Arc::new(Mutex::new(ClockMap::default())),
            ),
            dir,
        )
    }

    /// Test WAL delta resolution with just one missed operation on node C.
    ///
    /// See: <https://www.notion.so/qdrant/Testing-suite-4e28a978ec05476080ff26ed07757def?pvs=4>
    #[tokio::test]
    async fn test_resolve_wal_delta_one_operation() {
        // Create WALs for peer A, B and C
        let (a_wal, _a_wal_dir) = fixture_empty_wal();
        let (b_wal, _b_wal_dir) = fixture_empty_wal();
        let (c_wal, _c_wal_dir) = fixture_empty_wal();

        // Create clock set for peer A
        let mut a_clock_set = ClockSet::new();

        // Create operation on peer A
        let mut a_clock_0 = a_clock_set.get_clock();
        let clock_tick = a_clock_0.tick_once();
        let clock_tag = ClockTag::new(1, a_clock_0.id(), clock_tick);
        let bare_operation =
            CollectionUpdateOperations::PointOperation(PointOperations::UpsertPoints(
                PointInsertOperationsInternal::PointsList(vec![PointStruct {
                    id: 1.into(),
                    vector: vec![1.0, 2.0, 3.0].into(),
                    payload: None,
                }]),
            ));
        let operation = OperationWithClockTag::new(bare_operation, Some(clock_tag));

        // Write operations to peer A, B and C, and advance clocks
        let mut a_operation = operation.clone();
        let mut b_operation = operation.clone();
        let mut c_operation = operation.clone();
        a_wal.lock_and_write(&mut a_operation).await.unwrap();
        b_wal.lock_and_write(&mut b_operation).await.unwrap();
        c_wal.lock_and_write(&mut c_operation).await.unwrap();
        a_clock_0.advance_to(a_operation.clock_tag.unwrap().clock_tick);
        a_clock_0.advance_to(b_operation.clock_tag.unwrap().clock_tick);
        a_clock_0.advance_to(c_operation.clock_tag.unwrap().clock_tick);
        drop(a_clock_0);

        // Create operation on peer A
        let mut a_clock_0 = a_clock_set.get_clock();
        let clock_tick = a_clock_0.tick_once();
        let clock_tag = ClockTag::new(1, a_clock_0.id(), clock_tick);
        let bare_operation =
            CollectionUpdateOperations::PointOperation(PointOperations::UpsertPoints(
                PointInsertOperationsInternal::PointsList(vec![PointStruct {
                    id: 2.into(),
                    vector: vec![3.0, 2.0, 1.0].into(),
                    payload: None,
                }]),
            ));
        let operation = OperationWithClockTag::new(bare_operation, Some(clock_tag));

        // Write operations to peer A and B, not C, and advance clocks
        let mut a_operation = operation.clone();
        let mut b_operation = operation.clone();
        a_wal.lock_and_write(&mut a_operation).await.unwrap();
        b_wal.lock_and_write(&mut b_operation).await.unwrap();
        a_clock_0.advance_to(a_operation.clock_tag.unwrap().clock_tick);
        a_clock_0.advance_to(b_operation.clock_tag.unwrap().clock_tick);
        drop(a_clock_0);

        let c_recovery_point = c_wal.recovery_point().await;

        // Resolve delta on node A for node C, assert correctness
        let delta_from = a_wal
            .resolve_wal_delta(c_recovery_point.clone())
            .await
            .unwrap();
        assert_eq!(delta_from, 1);

        // Resolve delta on node B for node C, assert correctness
        let delta_from = b_wal
            .resolve_wal_delta(c_recovery_point.clone())
            .await
            .unwrap();
        assert_eq!(delta_from, 1);

        // Diff should have 1 operation, as C missed just one
        assert_eq!(b_wal.wal.lock().read(delta_from).count(), 1);

        // Recover WAL on node C by writing delta from node B to it
        c_wal.append_from(&b_wal, delta_from).await.unwrap();

        // WALs should match up perfectly now
        a_wal
            .wal
            .lock()
            .read(0)
            .zip(b_wal.wal.lock().read(0))
            .zip(c_wal.wal.lock().read(0))
            .for_each(|((a, b), c)| {
                assert_eq!(a, b);
                assert_eq!(b, c);
            });
    }

    /// Test WAL delta resolution with a many missed operations on node C.
    ///
    /// See: <https://www.notion.so/qdrant/Testing-suite-4e28a978ec05476080ff26ed07757def?pvs=4>
    #[tokio::test]
    async fn test_resolve_wal_delta_many_operations() {
        const N: usize = 5;
        const M: usize = 25;

        // Create WALs for peer A, B and C
        let (a_wal, _a_wal_dir) = fixture_empty_wal();
        let (b_wal, _b_wal_dir) = fixture_empty_wal();
        let (c_wal, _c_wal_dir) = fixture_empty_wal();

        // Create clock set for peer A
        let mut a_clock_set = ClockSet::new();

        // Create N operation on peer A
        for i in 0..N {
            let mut a_clock_0 = a_clock_set.get_clock();
            let clock_tick = a_clock_0.tick_once();
            let clock_tag = ClockTag::new(1, a_clock_0.id(), clock_tick);
            let bare_operation =
                CollectionUpdateOperations::PointOperation(PointOperations::UpsertPoints(
                    PointInsertOperationsInternal::PointsList(vec![PointStruct {
                        id: (i as u64).into(),
                        vector: vec![i as f32, -(i as f32), 0.0].into(),
                        payload: None,
                    }]),
                ));
            let operation = OperationWithClockTag::new(bare_operation, Some(clock_tag));

            // Write operations to peer A, B and C, and advance clocks
            let mut a_operation = operation.clone();
            let mut b_operation = operation.clone();
            let mut c_operation = operation.clone();
            a_wal.lock_and_write(&mut a_operation).await.unwrap();
            b_wal.lock_and_write(&mut b_operation).await.unwrap();
            c_wal.lock_and_write(&mut c_operation).await.unwrap();
            a_clock_0.advance_to(a_operation.clock_tag.unwrap().clock_tick);
            a_clock_0.advance_to(b_operation.clock_tag.unwrap().clock_tick);
            a_clock_0.advance_to(c_operation.clock_tag.unwrap().clock_tick);
        }

        // Create M operations on peer A, which are missed on node C
        for i in N..N + M {
            let mut a_clock_0 = a_clock_set.get_clock();
            let clock_tick = a_clock_0.tick_once();
            let clock_tag = ClockTag::new(1, a_clock_0.id(), clock_tick);
            let bare_operation =
                CollectionUpdateOperations::PointOperation(PointOperations::UpsertPoints(
                    PointInsertOperationsInternal::PointsList(vec![PointStruct {
                        id: (i as u64).into(),
                        vector: vec![i as f32, -(i as f32), 0.0].into(),
                        payload: None,
                    }]),
                ));
            let operation = OperationWithClockTag::new(bare_operation, Some(clock_tag));

            // Write operations to peer A and B, not C, and advance clocks
            let mut a_operation = operation.clone();
            let mut b_operation = operation.clone();
            a_wal.lock_and_write(&mut a_operation).await.unwrap();
            b_wal.lock_and_write(&mut b_operation).await.unwrap();
            a_clock_0.advance_to(a_operation.clock_tag.unwrap().clock_tick);
            a_clock_0.advance_to(b_operation.clock_tag.unwrap().clock_tick);
        }

        let c_recovery_point = c_wal.recovery_point().await;

        // Resolve delta on node A for node C, assert correctness
        let delta_from = a_wal
            .resolve_wal_delta(c_recovery_point.clone())
            .await
            .unwrap();
        assert_eq!(delta_from, N as u64);

        // Resolve delta on node B for node C, assert correctness
        let delta_from = b_wal.resolve_wal_delta(c_recovery_point).await.unwrap();
        assert_eq!(delta_from, N as u64);

        // Diff should have M operations, as node C missed M operations
        assert_eq!(b_wal.wal.lock().read(delta_from).count(), M);

        // Recover WAL on node C by writing delta from node B to it
        c_wal.append_from(&b_wal, delta_from).await.unwrap();

        // WALs should match up perfectly now
        a_wal
            .wal
            .lock()
            .read(0)
            .zip(b_wal.wal.lock().read(0))
            .zip(c_wal.wal.lock().read(0))
            .for_each(|((a, b), c)| {
                assert_eq!(a, b);
                assert_eq!(b, c);
            });
    }

    /// Test WAL delta resolution with a many intermixed operations on node C. Intermixed as in,
    /// from multiple nodes.
    ///
    /// See: <https://www.notion.so/qdrant/Testing-suite-4e28a978ec05476080ff26ed07757def?pvs=4>
    #[tokio::test]
    async fn test_resolve_wal_delta_many_intermixed_operations() {
        const N: usize = 3;
        const M: usize = 50;

        // Create WALs for peer A, B and C
        let (a_wal, _a_wal_dir) = fixture_empty_wal();
        let (b_wal, _b_wal_dir) = fixture_empty_wal();
        let (c_wal, _c_wal_dir) = fixture_empty_wal();

        // Create clock sets for peer A and B
        let mut a_clock_set = ClockSet::new();
        let mut b_clock_set = ClockSet::new();

        // Create N operation on peer A
        for i in 0..N {
            let mut a_clock_0 = a_clock_set.get_clock();
            let clock_tick = a_clock_0.tick_once();
            let clock_tag = ClockTag::new(1, a_clock_0.id(), clock_tick);
            let bare_operation =
                CollectionUpdateOperations::PointOperation(PointOperations::UpsertPoints(
                    PointInsertOperationsInternal::PointsList(vec![PointStruct {
                        id: (i as u64).into(),
                        vector: vec![i as f32, -(i as f32), 0.0].into(),
                        payload: None,
                    }]),
                ));
            let operation = OperationWithClockTag::new(bare_operation, Some(clock_tag));

            // Write operations to peer A, B and C, and advance clocks
            let mut a_operation = operation.clone();
            let mut b_operation = operation.clone();
            let mut c_operation = operation.clone();
            a_wal.lock_and_write(&mut a_operation).await.unwrap();
            b_wal.lock_and_write(&mut b_operation).await.unwrap();
            c_wal.lock_and_write(&mut c_operation).await.unwrap();
            a_clock_0.advance_to(a_operation.clock_tag.unwrap().clock_tick);
            a_clock_0.advance_to(b_operation.clock_tag.unwrap().clock_tick);
            a_clock_0.advance_to(c_operation.clock_tag.unwrap().clock_tick);
        }

        // Create M operations on peer A, which are missed on node C
        for i in N..N + M {
            let is_node_a = i % 3 == 0;
            let peer_id = if is_node_a { 1 } else { 2 };

            let mut clock = if is_node_a {
                a_clock_set.get_clock()
            } else {
                b_clock_set.get_clock()
            };
            let clock_tick = clock.tick_once();
            let clock_tag = ClockTag::new(peer_id, clock.id(), clock_tick);
            let bare_operation =
                CollectionUpdateOperations::PointOperation(PointOperations::UpsertPoints(
                    PointInsertOperationsInternal::PointsList(vec![PointStruct {
                        id: (i as u64).into(),
                        vector: vec![i as f32, -(i as f32), 0.0].into(),
                        payload: None,
                    }]),
                ));
            let operation = OperationWithClockTag::new(bare_operation, Some(clock_tag));

            // Write operations to peer A and B, not C, and advance clocks
            let mut a_operation = operation.clone();
            let mut b_operation = operation.clone();
            a_wal.lock_and_write(&mut a_operation).await.unwrap();
            b_wal.lock_and_write(&mut b_operation).await.unwrap();
            clock.advance_to(a_operation.clock_tag.unwrap().clock_tick);
            clock.advance_to(b_operation.clock_tag.unwrap().clock_tick);
        }

        let c_recovery_point = c_wal.recovery_point().await;

        // Resolve delta on node A for node C, assert correctness
        let delta_from = a_wal
            .resolve_wal_delta(c_recovery_point.clone())
            .await
            .unwrap();
        assert_eq!(delta_from, N as u64);

        // Resolve delta on node B for node C, assert correctness
        let delta_from = b_wal.resolve_wal_delta(c_recovery_point).await.unwrap();
        assert_eq!(delta_from, N as u64);

        // Diff should have M operations, as node C missed M operations
        assert_eq!(b_wal.wal.lock().read(delta_from).count(), M);

        // Recover WAL on node C by writing delta from node B to it
        c_wal.append_from(&b_wal, delta_from).await.unwrap();

        // WALs should match up perfectly now
        a_wal
            .wal
            .lock()
            .read(0)
            .zip(b_wal.wal.lock().read(0))
            .zip(c_wal.wal.lock().read(0))
            .for_each(|((a, b), c)| {
                assert_eq!(a, b);
                assert_eq!(b, c);
            });
    }

    /// Test WAL delta resolution with operations in a different order on node A and B.
    ///
    /// See: <https://www.notion.so/qdrant/Testing-suite-4e28a978ec05476080ff26ed07757def?pvs=4>
    #[tokio::test]
    async fn test_resolve_wal_delta_unordered_operations() {
        // Create WALs for peer A, B and C
        let (a_wal, _a_wal_dir) = fixture_empty_wal();
        let (b_wal, _b_wal_dir) = fixture_empty_wal();
        let (c_wal, _c_wal_dir) = fixture_empty_wal();

        // Create clock sets for peer A and B
        let mut a_clock_set = ClockSet::new();
        let mut b_clock_set = ClockSet::new();

        // Create operation on peer A
        let mut a_clock_0 = a_clock_set.get_clock();
        let clock_tick = a_clock_0.tick_once();
        let clock_tag = ClockTag::new(1, a_clock_0.id(), clock_tick);
        let bare_operation =
            CollectionUpdateOperations::PointOperation(PointOperations::UpsertPoints(
                PointInsertOperationsInternal::PointsList(vec![PointStruct {
                    id: 1.into(),
                    vector: vec![1.0, 2.0, 3.0].into(),
                    payload: None,
                }]),
            ));
        let operation = OperationWithClockTag::new(bare_operation, Some(clock_tag));

        // Write operations to peer A, B and C, and advance clocks
        let mut a_operation = operation.clone();
        let mut b_operation = operation.clone();
        let mut c_operation = operation.clone();
        a_wal.lock_and_write(&mut a_operation).await.unwrap();
        b_wal.lock_and_write(&mut b_operation).await.unwrap();
        c_wal.lock_and_write(&mut c_operation).await.unwrap();
        a_clock_0.advance_to(a_operation.clock_tag.unwrap().clock_tick);
        a_clock_0.advance_to(b_operation.clock_tag.unwrap().clock_tick);
        a_clock_0.advance_to(c_operation.clock_tag.unwrap().clock_tick);
        drop(a_clock_0);

        // Create an operation on node A and B
        let mut a_clock_0 = a_clock_set.get_clock();
        let mut b_clock_0 = b_clock_set.get_clock();
        let a_clock_tick = a_clock_0.tick_once();
        let b_clock_tick = b_clock_0.tick_once();
        let a_clock_tag = ClockTag::new(1, a_clock_0.id(), a_clock_tick);
        let b_clock_tag = ClockTag::new(2, a_clock_0.id(), b_clock_tick);
        let bare_operation_1 =
            CollectionUpdateOperations::PointOperation(PointOperations::UpsertPoints(
                PointInsertOperationsInternal::PointsList(vec![PointStruct {
                    id: 2.into(),
                    vector: vec![3.0, 2.0, 1.0].into(),
                    payload: None,
                }]),
            ));
        let bare_operation_2 =
            CollectionUpdateOperations::PointOperation(PointOperations::UpsertPoints(
                PointInsertOperationsInternal::PointsList(vec![PointStruct {
                    id: 3.into(),
                    vector: vec![9.0, 8.0, 7.0].into(),
                    payload: None,
                }]),
            ));
        let operation_1 = OperationWithClockTag::new(bare_operation_1, Some(a_clock_tag));
        let operation_2 = OperationWithClockTag::new(bare_operation_2, Some(b_clock_tag));

        // Write operations to node A and B in different order, but not on node C
        let mut a_operation_1 = operation_1.clone();
        let mut a_operation_2 = operation_2.clone();
        let mut b_operation_1 = operation_1.clone();
        let mut b_operation_2 = operation_2.clone();
        a_wal.lock_and_write(&mut a_operation_1).await.unwrap();
        a_wal.lock_and_write(&mut a_operation_2).await.unwrap();
        b_wal.lock_and_write(&mut b_operation_2).await.unwrap();
        b_wal.lock_and_write(&mut b_operation_1).await.unwrap();
        a_clock_0.advance_to(a_operation_1.clock_tag.unwrap().clock_tick);
        a_clock_0.advance_to(a_operation_2.clock_tag.unwrap().clock_tick);
        b_clock_0.advance_to(b_operation_2.clock_tag.unwrap().clock_tick);
        b_clock_0.advance_to(b_operation_1.clock_tag.unwrap().clock_tick);
        drop(a_clock_0);
        drop(b_clock_0);

        let c_recovery_point = c_wal.recovery_point().await;

        // Resolve delta on node A for node C, assert correctness
        let delta_from = a_wal
            .resolve_wal_delta(c_recovery_point.clone())
            .await
            .unwrap();
        assert_eq!(delta_from, 1);

        // Resolve delta on node B for node C, assert correctness
        let delta_from = b_wal
            .resolve_wal_delta(c_recovery_point.clone())
            .await
            .unwrap();
        assert_eq!(delta_from, 1);

        // Diff should have 2 operations on both nodes
        assert_eq!(a_wal.wal.lock().read(delta_from).count(), 2);
        assert_eq!(b_wal.wal.lock().read(delta_from).count(), 2);

        // Recover WAL on node C by writing delta from node B to it
        c_wal.append_from(&b_wal, delta_from).await.unwrap();

        // WAL on node B and C will match, A is in different order
        assert!(!a_wal
            .wal
            .lock()
            .read(0)
            .zip(c_wal.wal.lock().read(0))
            .all(|(a, c)| a == c));
        assert!(b_wal
            .wal
            .lock()
            .read(0)
            .zip(c_wal.wal.lock().read(0))
            .all(|(b, c)| b == c));

        // All WALs should have 3 operations
        assert_eq!(a_wal.wal.lock().read(0).count(), 3);
        assert_eq!(b_wal.wal.lock().read(0).count(), 3);
        assert_eq!(c_wal.wal.lock().read(0).count(), 3);

        // All WALs must have operations for point 1, 2 and 3
        let get_point = |op| match op {
            OperationWithClockTag {
                operation:
                    CollectionUpdateOperations::PointOperation(PointOperations::UpsertPoints(
                        PointInsertOperationsInternal::PointsList(points),
                    )),
                ..
            } => points[0].clone(),
            _ => unreachable!(),
        };
        let a_wal_point_ids = a_wal
            .wal
            .lock()
            .read(0)
            .map(|(_, op)| get_point(op).id)
            .collect::<HashSet<_>>();
        let b_wal_point_ids = b_wal
            .wal
            .lock()
            .read(0)
            .map(|(_, op)| get_point(op).id)
            .collect::<HashSet<_>>();
        let c_wal_point_ids = c_wal
            .wal
            .lock()
            .read(0)
            .map(|(_, op)| get_point(op).id)
            .collect::<HashSet<_>>();
        (1..=3).for_each(|i| {
            assert!(a_wal_point_ids.contains(&i.into()));
            assert!(b_wal_point_ids.contains(&i.into()));
            assert!(c_wal_point_ids.contains(&i.into()));
        });
    }

    /// Empty recovery point should not resolve any diff.
    #[test]
    fn test_empty_recovery_point() {
        let wal_dir = Builder::new().prefix("wal_test").tempdir().unwrap();
        let wal_options = WalOptions {
            segment_capacity: 1024 * 1024,
            segment_queue_len: 0,
        };
        let wal: SerdeWal<OperationWithClockTag> =
            SerdeWal::new(wal_dir.path().to_str().unwrap(), wal_options).unwrap();
        let wal = Arc::new(ParkingMutex::new(wal));

        // Empty recovery points, should not resolve any diff
        let recovery_point = RecoveryPoint::default();
        let local_recovery_point = RecoveryPoint::default();

        let resolve_result = resolve_wal_delta(recovery_point, wal, &local_recovery_point);
        assert_eq!(
            resolve_result.unwrap_err().to_string(),
            "recovery point has no clocks to resolve delta for",
        );
    }

    /// Recovery point with a clock our source does not know about cannot resolve a diff.
    #[test]
    fn test_recover_point_has_unknown_clock() {
        let wal_dir = Builder::new().prefix("wal_test").tempdir().unwrap();
        let wal_options = WalOptions {
            segment_capacity: 1024 * 1024,
            segment_queue_len: 0,
        };
        let wal: SerdeWal<OperationWithClockTag> =
            SerdeWal::new(wal_dir.path().to_str().unwrap(), wal_options).unwrap();
        let wal = Arc::new(ParkingMutex::new(wal));

        let mut recovery_point = RecoveryPoint::default();
        let mut local_recovery_point = RecoveryPoint::default();

        // Recovery point has a clock our source does not know about
        recovery_point.insert(1, 0, 15);
        recovery_point.insert(1, 1, 8);
        recovery_point.insert(2, 1, 5);
        local_recovery_point.insert(1, 0, 20);
        local_recovery_point.insert(1, 1, 8);

        let resolve_result = resolve_wal_delta(recovery_point, wal, &local_recovery_point);
        assert_eq!(
            resolve_result.unwrap_err().to_string(),
            "recovery point requests clocks this WAL does not know about",
        );
    }

    /// Recovery point with higher clocks than the source cannot resolve a diff.
    #[test]
    fn test_recover_point_higher_than_source() {
        let wal_dir = Builder::new().prefix("wal_test").tempdir().unwrap();
        let wal_options = WalOptions {
            segment_capacity: 1024 * 1024,
            segment_queue_len: 0,
        };
        let wal: SerdeWal<OperationWithClockTag> =
            SerdeWal::new(wal_dir.path().to_str().unwrap(), wal_options).unwrap();
        let wal = Arc::new(ParkingMutex::new(wal));

        let mut recovery_point = RecoveryPoint::default();
        let mut local_recovery_point = RecoveryPoint::default();

        // Recovery point asks tick 10, but source only has tick 8
        recovery_point.insert(1, 0, 15);
        recovery_point.insert(1, 1, 10);
        local_recovery_point.insert(1, 0, 20);
        local_recovery_point.insert(1, 1, 8);

        let resolve_result = resolve_wal_delta(recovery_point, wal, &local_recovery_point);
        assert_eq!(
            resolve_result.unwrap_err().to_string(),
            "recovery point requests higher clocks this WAL has",
        );
    }

    // TODO: implement this once we have a truncation clock map
    // /// Recovery point requests clocks that are already truncated
    // #[test]
    // fn test_recover_point_truncated() {
    //     let wal_dir = Builder::new().prefix("wal_test").tempdir().unwrap();
    //     let wal_options = WalOptions {
    //         segment_capacity: 1024 * 1024,
    //         segment_queue_len: 0,
    //     };
    //     let wal: SerdeWal<OperationWithClockTag> =
    //         SerdeWal::new(wal_dir.path().to_str().unwrap(), wal_options).unwrap();
    //     let wal = Arc::new(ParkingMutex::new(wal));

    //     let mut recovery_point = RecoveryPoint::default();
    //     let mut local_recovery_point = RecoveryPoint::default();

    //     // Recovery point asks clock tick that has been truncated already
    //     recovery_point.insert(1, 0, 15);
    //     recovery_point.insert(1, 1, 10);
    //     local_recovery_point.insert(1, 0, 20);
    //     local_recovery_point.insert(1, 1, 12);

    //     let resolve_result = resolve_wal_delta(recovery_point, wal, local_recovery_point);
    //     assert_eq!(
    //         resolve_result.unwrap_err().to_string(),
    //         "recovery point requests clocks truncated from this WAL",
    //     );
    // }

    /// Recovery point operations are not in our WAL.
    #[test]
    fn test_recover_point_not_in_wal() {
        let wal_dir = Builder::new().prefix("wal_test").tempdir().unwrap();
        let wal_options = WalOptions {
            segment_capacity: 1024 * 1024,
            segment_queue_len: 0,
        };
        let wal: SerdeWal<OperationWithClockTag> =
            SerdeWal::new(wal_dir.path().to_str().unwrap(), wal_options).unwrap();
        let wal = Arc::new(ParkingMutex::new(wal));

        let mut recovery_point = RecoveryPoint::default();
        let mut local_recovery_point = RecoveryPoint::default();

        // Recovery point asks tick 10, but source only has tick 8
        recovery_point.insert(1, 0, 15);
        recovery_point.insert(1, 1, 10);
        local_recovery_point.insert(1, 0, 20);
        local_recovery_point.insert(1, 1, 12);

        let resolve_result = resolve_wal_delta(recovery_point, wal, &local_recovery_point);
        assert_eq!(
            resolve_result.unwrap_err().to_string(),
            "cannot find slice of WAL operations that satisfies the recovery point",
        );
    }
}

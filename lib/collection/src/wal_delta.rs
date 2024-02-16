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
            self.recovery_point().await,
        )
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
    local_recovery_point: RecoveryPoint,
) -> Result<u64, WalDeltaError> {
    // If the recovery point has clocks our current node does not know about
    // we're missing essential operations and cannot resolve a WAL delta
    if recovery_point.has_clocks_not_in(&local_recovery_point) {
        return Err(WalDeltaError::UnknownClocks);
    }

    // If our current node has any lower clock than the recovery point specifies,
    // we're missing essential operations and cannot resolve a WAL delta
    if recovery_point.has_any_higher(&local_recovery_point) {
        return Err(WalDeltaError::HigherThanCurrent);
    }

    // Extend clock map with missing clocks this node know about
    // Ensure the recovering node gets records for a clock it might not have seen yet
    recovery_point.extend_with_missing_clocks(&local_recovery_point);

    // If recovery point is empty, we cannot do a diff transfer
    if recovery_point.is_empty() {
        return Err(WalDeltaError::Empty);
    }

    // Remove clocks that are equal to this node, we don't have to transfer records for them
    // TODO: do we want to remove higher clocks too, as the recovery node already has all data?
    recovery_point.remove_equal_clocks(&local_recovery_point);

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

    fn fixture_empty_wal() -> (SerdeWal<OperationWithClockTag>, TempDir) {
        let dir = Builder::new().prefix("wal_test").tempdir().unwrap();
        let options = WalOptions {
            segment_capacity: 1024 * 1024,
            segment_queue_len: 0,
        };
        (SerdeWal::new(dir.path().to_str().unwrap(), options).unwrap(), dir)
    }

    /// Test WAL delta resolution with just one missed operation on node C.
    ///
    /// Simplified version of: <https://www.notion.so/qdrant/Testing-suite-4e28a978ec05476080ff26ed07757def?pvs=4>
    #[test]
    fn test_resolve_wal_one_operation() {
        // Create WALs for peer A, B and C
        let (mut a_wal, _a_wal_dir) = fixture_empty_wal();
        let (mut b_wal, _b_wal_dir) = fixture_empty_wal();
        let (mut c_wal, _c_wal_dir) = fixture_empty_wal();

        // Create clock sets for peer A, B and C
        let mut a_clock_set = ClockSet::new();
        let mut a_clock_map = ClockMap::default();
        let mut b_clock_map = ClockMap::default();
        let mut c_clock_map = ClockMap::default();

        // Create operation on peer A
        let mut a_clock_0 = a_clock_set.get_clock();
        let clock_tick = a_clock_0.tick_once();
        let clock_tag = ClockTag::new(1, 0, clock_tick);
        let operation = CollectionUpdateOperations::PointOperation(PointOperations::UpsertPoints(
            PointInsertOperationsInternal::PointsList(vec![PointStruct {
                id: 1.into(),
                vector: vec![1.0, 2.0, 3.0].into(),
                payload: None,
            }]),
        ));
        let operation_with_clock_tag = OperationWithClockTag::new(operation, Some(clock_tag));

        // Write operations to peer A, B and C, and advance clocks
        a_wal.write(&operation_with_clock_tag).unwrap();
        b_wal.write(&operation_with_clock_tag).unwrap();
        c_wal.write(&operation_with_clock_tag).unwrap();
        a_clock_0.advance_to(0);
        drop(a_clock_0);
        a_clock_map.advance_clock(clock_tag);
        b_clock_map.advance_clock(clock_tag);
        c_clock_map.advance_clock(clock_tag);

        // Create operation on peer A
        let mut a_clock_0 = a_clock_set.get_clock();
        let clock_tick = a_clock_0.tick_once();
        let clock_tag = ClockTag::new(1, 0, clock_tick);
        let operation = CollectionUpdateOperations::PointOperation(PointOperations::UpsertPoints(
            PointInsertOperationsInternal::PointsList(vec![PointStruct {
                id: 2.into(),
                vector: vec![3.0, 2.0, 1.0].into(),
                payload: None,
            }]),
        ));
        let operation_with_clock_tag = OperationWithClockTag::new(operation, Some(clock_tag));

        // Write operations to peer A and B, not C, and advance clocks
        a_wal.write(&operation_with_clock_tag).unwrap();
        b_wal.write(&operation_with_clock_tag).unwrap();
        a_clock_0.advance_to(0);
        drop(a_clock_0);
        a_clock_map.advance_clock(clock_tag);
        b_clock_map.advance_clock(clock_tag);

        let b_wal = Arc::new(ParkingMutex::new(b_wal));
        let b_recovery_point = b_clock_map.to_recovery_point();
        let c_recovery_point = c_clock_map.to_recovery_point();

        // Recover node C from node B, assert delta point is correct
        let delta_from = resolve_wal_delta(c_recovery_point, b_wal.clone(), b_recovery_point).unwrap();
        assert_eq!(delta_from, 1);

        // Recover WAL on node C by writing delta from node B to it
        b_wal.lock().read(delta_from).for_each(|(_, update)| {
            c_wal.write(&update).unwrap();
        });

        // WALs should match up perfectly now
        a_wal
            .read(0)
            .zip(b_wal.lock().read(0))
            .zip(c_wal.read(0))
            .for_each(|((a, b), c)| {
                assert_eq!(a, b);
                assert_eq!(b, c);
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

        let resolve_result = resolve_wal_delta(recovery_point, wal, local_recovery_point);
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

        let resolve_result = resolve_wal_delta(recovery_point, wal, local_recovery_point);
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

        let resolve_result = resolve_wal_delta(recovery_point, wal, local_recovery_point);
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

        let resolve_result = resolve_wal_delta(recovery_point, wal, local_recovery_point);
        assert_eq!(
            resolve_result.unwrap_err().to_string(),
            "cannot find slice of WAL operations that satisfies the recovery point",
        );
    }
}

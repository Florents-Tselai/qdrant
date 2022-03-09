use crate::types::{ScoreType, VectorElementType};

use std::arch::aarch64::*;

#[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
pub unsafe fn euclid_similarity_neon(
    v1: &[VectorElementType],
    v2: &[VectorElementType],
) -> ScoreType {
    let n = v1.len();
    let m = n - (n % 4);
    let zeros: [f32; 4] = [0.0; 4];
    let mut sum = vld1q_f32(&zeros[0]);
    for i in (0..m).step_by(4) {
        let a = vld1q_f32(&v1[i]);
        let b = vld1q_f32(&v2[i]);
        let sub = vsubq_f32(a, b);
        sum = vfmaq_f32(sum, sub, sub);
    }
    let mut res = vaddvq_f32(sum);
    for i in m..n {
        res += (v1[i] - v2[i]).powi(2);
    }
    -res.sqrt()
}

#[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
pub unsafe fn cosine_preprocess_neon(vector: &[VectorElementType]) -> Vec<VectorElementType> {
    let n = vector.len();
    let m = n - (n % 4);
    let zeros: [f32; 4] = [0.0; 4];
    let mut sum = vld1q_f32(&zeros[0]);
    for i in (0..m).step_by(4) {
        let a = vld1q_f32(&vector[i]);
        sum = vfmaq_f32(sum, a, a);
    }
    let mut length = vaddvq_f32(sum);
    for v in vector.iter().take(n).skip(m) {
        length += v.powi(2);
    }
    let length = length.sqrt();
    vector.iter().map(|x| x / length).collect()
}

#[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
pub unsafe fn dot_similarity_neon(v1: &[VectorElementType], v2: &[VectorElementType]) -> ScoreType {
    let n = v1.len();
    let m = n - (n % 4);
    let zeros: [f32; 4] = [0.0; 4];
    let mut sum = vld1q_f32(&zeros[0]);
    for i in (0..m).step_by(4) {
        let a = vld1q_f32(&v1[i]);
        let b = vld1q_f32(&v2[i]);
        sum = vfmaq_f32(sum, a, b);
    }
    let mut res = vaddvq_f32(sum);
    for i in m..n {
        res += v1[i] * v2[i];
    }
    res
}

#[cfg(test)]
mod tests {
    #[cfg(target_feature = "neon")]
    #[test]
    fn test_spaces_neon() {
        use super::*;
        use crate::spaces::simple::*;

        if std::arch::is_aarch64_feature_detected!("neon") {
            let v1: Vec<f32> = vec![
                10., 11., 12., 13., 14., 15., 16., 17., 18., 19., 20., 21., 22., 23., 24., 25.,
                26., 27., 28., 29., 30., 31.,
            ];
            let v2: Vec<f32> = vec![
                40., 41., 42., 43., 44., 45., 46., 47., 48., 49., 50., 51., 52., 53., 54., 55.,
                56., 57., 58., 59., 60., 61.,
            ];

            let euclid_simd = unsafe { euclid_similarity_neon(&v1, &v2) };
            let euclid = euclid_similarity(&v1, &v2);
            assert_eq!(euclid_simd, euclid);

            let dot_simd = unsafe { dot_similarity_neon(&v1, &v2) };
            let dot = dot_similarity(&v1, &v2);
            assert_eq!(dot_simd, dot);

            let cosine_simd = unsafe { cosine_preprocess_neon(&v1) };
            let cosine = cosine_preprocess(&v1);
            assert_eq!(cosine_simd, cosine);
        } else {
            println!("neon test skipped");
        }
    }
}

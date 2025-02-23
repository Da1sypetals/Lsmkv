use crate::memory::types::Key;

use super::search::Index;

#[derive(Debug, PartialEq, Clone, Copy)]
/// This is index in the Index array.
pub enum BisectResult {
    //
    //                 Exact(idx)
    //                 |
    //                 v
    // +--------------+--------------+--------------+
    // | key, offset  | key, offset  | key, offset  |
    // +--------------+--------------+--------------+
    //
    /// Use: index[idx] to get the exact offset
    Exact(usize),
    //
    //                 left                          right
    //                 |                             |
    //                 v                             v
    // +--------------+--------------+--------------+--------------+
    // | key, offset  | key, offset  | key, offset  | key, offset  |
    // +--------------+--------------+--------------+--------------+
    //
    /// Use: index[left], index[right] to get the range of two offsets
    Range(usize, usize),
}

/// Returns:
///  - None if the key is not found
///  - Some(BisectResult::Exact(idx)) if the key is found exactly
///  - Some(BisectResult::Range(left, right)) if the key is not found but in the range of two keys in Index array
pub(crate) fn bisect_index(index: &Index, key: &Key) -> Option<BisectResult> {
    if index.is_empty() {
        return None;
    }

    let mut left = 0;
    let mut right = index.len() - 1;

    // If key is outside the range of all keys
    if key.key < &index[0].0.key || key > &index[right].0 {
        return None;
    } else if key.key == &index[0].0.key && key.timestamp > index[0].0.timestamp {
        return Some(BisectResult::Range(0, 1));
    }

    // Binary search
    while left <= right {
        let mid = left + (right - left) / 2;
        match key.cmp(&index[mid].0) {
            std::cmp::Ordering::Equal => return Some(BisectResult::Exact(mid)),
            std::cmp::Ordering::Less => {
                if mid == 0 {
                    return None;
                }
                right = mid - 1;
            }
            std::cmp::Ordering::Greater => {
                if mid == index.len() - 1 {
                    return None;
                }
                left = mid + 1;
            }
        }
    }

    // If we get here, the key wasn't found exactly
    // left is now the insertion point
    // right is the largest element smaller than key
    Some(BisectResult::Range(right, left))
}

// use super::search::Index;
// use crate::memory::types::Key;

// /// implements lower bound
// /// returns the index in the `index` vector.
// /// if: index[0].0.key > key.key, or index[last].0 < key, return None.
// pub fn bisect_index(index: &Index, key: &Key) -> Option<usize> {
//     if index.is_empty() {
//         return None;
//     }

//     // Check if key is outside the range
//     if key.key < &index[0].0.key || key > &index[index.len() - 1].0 {
//         return None;
//     }

//     let mut left = 0;
//     let mut right = index.len() - 1;

//     while left < right {
//         let mid = left + (right - left) / 2;
//         match key.cmp(&index[mid].0) {
//             std::cmp::Ordering::Equal => return Some(mid),
//             std::cmp::Ordering::Less => {
//                 if mid == 0 {
//                     return None;
//                 }
//                 right = mid;
//             }
//             std::cmp::Ordering::Greater => {
//                 left = mid + 1;
//             }
//         }
//     }

//     // At this point, left is the insertion point (lower bound)
//     if left < index.len() { Some(left) } else { None }
// }

// fn lower_bound<T: Ord>(arr: &[T], target: T) -> usize {
//     let mut left = 0;
//     let mut right = arr.len();

//     while left < right {
//         let mid = left + (right - left) / 2;
//         if arr[mid] < target {
//             left = mid + 1;
//         } else {
//             right = mid;
//         }
//     }
//     left
// }

// #[cfg(test)]
// mod tests {
//     use super::*;

//     #[test]
//     fn test_lower_bound() {
//         let arr = vec![1, 2, 4, 4, 5, 6];

//         assert_eq!(lower_bound(&arr, 0), 0);
//         assert_eq!(lower_bound(&arr, 1), 0);
//         assert_eq!(lower_bound(&arr, 2), 1);
//         assert_eq!(lower_bound(&arr, 3), 2);
//         assert_eq!(lower_bound(&arr, 4), 2);
//         assert_eq!(lower_bound(&arr, 5), 4);
//         assert_eq!(lower_bound(&arr, 6), 5);
//         assert_eq!(lower_bound(&arr, 7), 6);
//     }

//     #[test]
//     fn test_lower_bound_empty() {
//         let arr: Vec<i32> = vec![];
//         assert_eq!(lower_bound(&arr, 5), 0);
//     }

//     #[test]
//     fn test_lower_bound_single_element() {
//         let arr = vec![10];
//         assert_eq!(lower_bound(&arr, 5), 0);
//         assert_eq!(lower_bound(&arr, 10), 0);
//         assert_eq!(lower_bound(&arr, 15), 1);
//     }

//     #[test]
//     fn test_lower_bound_all_same() {
//         let arr = vec![5, 5, 5, 5];
//         assert_eq!(lower_bound(&arr, 4), 0);
//         assert_eq!(lower_bound(&arr, 5), 0);
//         assert_eq!(lower_bound(&arr, 6), 4);
//     }
// }

// fn main() {
//     // Example usage
//     let arr = vec![1, 3, 5, 7, 9];
//     println!("{}", lower_bound(&arr, 5)); // Output: 2
// }

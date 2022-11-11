fn merge<T: Copy + Ord>(left_half: &[T], right_half: &[T], y: &mut [T]) {
    let mut l = 0;
    let mut r = 0;
    let mut i = 0;

    while l < left_half.len() && r < right_half.len() {
        if left_half[l] < right_half[r] {
            y[i] = left_half[l];
            l += 1;
        }else{
            y[i] = right_half[r];
            r += 1;
        }
        i += 1;
    }

    // get all remaining items if any
    if l < left_half.len(){
        y[i..].copy_from_slice(&left_half[l..]);
    }
    if r < right_half.len(){
        y[i..].copy_from_slice(&right_half[r..]); }
}

pub fn merge_sort<T: Copy + Ord>(original_arr: &mut [T]) {
        let len = original_arr.len();
        if len <= 1{
            return;
        }

        let mid = len / 2;

        merge_sort(&mut original_arr[..mid]);
        merge_sort(&mut original_arr[mid..]);

        let mut working_arr: Vec<T> = original_arr.to_vec();

        merge(&original_arr[..mid], &original_arr[mid..], &mut working_arr[..]);

        original_arr.copy_from_slice(&working_arr);
}

#[cfg(test)]
mod tests{
    use super::*;

    #[test]
    fn sorting(){
        let mut test_vec = vec![5, 1 , 2, 4, 3];
        merge_sort(&mut test_vec);
        assert_eq!(test_vec, vec![1, 2, 3, 4, 5]);

        let mut test_vec = vec![5, 1 , 2, 4, 3];
        merge_sort(&mut test_vec);
        assert_eq!(test_vec, vec![1, 2, 3, 4, 5]);

        let mut test_vec = vec![5, 1 , 2, 3, 4];
        merge_sort(&mut test_vec);
        assert_eq!(test_vec, vec![1, 2, 3, 4, 5]);
    }

    #[test]
    fn empty(){
        let mut test_vec: Vec<u64> = Vec::new();
        merge_sort(&mut test_vec);
        assert_eq!(test_vec.len(), 0);
    }
}

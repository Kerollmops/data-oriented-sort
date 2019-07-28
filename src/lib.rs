#![cfg_attr(feature = "nightly", feature(test))]

use std::iter::FromIterator;
use std::fmt;

use rand::{Rng, SeedableRng};
use rand::distributions::Standard;

#[derive(Debug, Clone, PartialOrd, Ord, PartialEq, Eq)]
struct Classic {
    query_index: u32,
    distance: u8,
    attribute: u16,
    word_index: u16,
    is_exact: bool,
}

fn new_classics<R: Rng + Clone>(rng: R, len: usize) -> Vec<Classic> {
    let mut query_index = rng.clone().sample_iter(Standard);
    let mut distance = rng.clone().sample_iter(Standard);
    let mut attribute = rng.clone().sample_iter(Standard);
    let mut word_index = rng.clone().sample_iter(Standard);
    let mut is_exact = rng.clone().sample_iter(Standard);

    let mut classics = Vec::with_capacity(len);

    for _ in 0..len {
        let query_index = query_index.next().unwrap();
        let distance = distance.next().unwrap();
        let attribute = attribute.next().unwrap();
        let word_index = word_index.next().unwrap();
        let is_exact = is_exact.next().unwrap();

        let classic = Classic { query_index, distance, attribute, word_index, is_exact };
        classics.push(classic);
    }

    classics
}

#[derive(Clone, PartialOrd, Ord, PartialEq, Eq)]
struct DataOriented {
    query_index: Vec<u32>,
    distance: Vec<u8>,
    attribute: Vec<u16>,
    word_index: Vec<u16>,
    is_exact: Vec<bool>,
}

impl fmt::Debug for DataOriented {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        writeln!(fmt, "DataOriented {{")?;
            writeln!(fmt, "    query_index: {:?}", &self.query_index)?;
            writeln!(fmt, "    distance: {:?}", &self.distance)?;
            writeln!(fmt, "    attribute: {:?}", &self.attribute)?;
            writeln!(fmt, "    word_index: {:?}", &self.word_index)?;
            writeln!(fmt, "    is_exact: {:?}", &self.is_exact)?;
        writeln!(fmt, "}}")?;

        Ok(())
    }
}

impl DataOriented {
    fn new<R: Rng + Clone>(rng: R, len: usize) -> DataOriented {
        let query_index = rng.clone().sample_iter(Standard).take(len);
        let distance = rng.clone().sample_iter(Standard).take(len);
        let attribute = rng.clone().sample_iter(Standard).take(len);
        let word_index = rng.clone().sample_iter(Standard).take(len);
        let is_exact = rng.clone().sample_iter(Standard).take(len);

        DataOriented {
            query_index: Vec::from_iter(query_index),
            distance: Vec::from_iter(distance),
            attribute: Vec::from_iter(attribute),
            word_index: Vec::from_iter(word_index),
            is_exact: Vec::from_iter(is_exact),
        }
    }

    fn len(&self) -> usize {
        self.query_index.len()
    }
}

fn permutations_unstable_by_key<F, K>(len: usize, mut f: F) -> Vec<usize>
where F: FnMut(usize) -> K,
      K: Ord,
{
    let mut permutations: Vec<usize> = (0..len).collect();
    permutations.sort_unstable_by_key(|&i| f(i));
    permutations
}

// this function is O(N) in term of memory but it could be O(1)
// by following this blog post
// https://devblogs.microsoft.com/oldnewthing/20170102-00/?p=95095
fn apply_permutations<T: Clone>(permutations: &[usize], vec: &mut Vec<T>) {
    assert_eq!(permutations.len(), vec.len());

    // it is not necessary to restrict items to be Clone,
    // we could ptr::read and, after having copied everything,
    // set_len to 0 and drop the "empty" vec.
    let mut new = Vec::with_capacity(permutations.len());
    for &i in permutations {
        let elem = unsafe { vec.get_unchecked(i) };
        new.push(elem.clone());
    }
    std::mem::replace(vec, new);
}

#[cfg(test)]
mod tests {
    use super::*;

    use rand::rngs::StdRng;

    #[test]
    fn data_oriented_sort_is_valid() {
        let length = 16_000;

        let rng = StdRng::from_seed([42; 32]);
        let mut classics = new_classics(rng, length);

        let rng = StdRng::from_seed([42; 32]);
        let mut data_oriented = DataOriented::new(rng, length);

        // before sort
        for i in 0..length {
            let classic = &classics[i];
            assert_eq!(classic.query_index, data_oriented.query_index[i]);
            assert_eq!(classic.distance,    data_oriented.distance[i]);
            assert_eq!(classic.attribute,   data_oriented.attribute[i]);
            assert_eq!(classic.word_index,  data_oriented.word_index[i]);
            assert_eq!(classic.is_exact,    data_oriented.is_exact[i]);
        }

        // sort classics
        classics.sort_unstable();

        // sort data oriented
        let permutations = permutations_unstable_by_key(data_oriented.len(), |i| unsafe {
            (
                data_oriented.query_index.get_unchecked(i),
                data_oriented.distance.get_unchecked(i),
                data_oriented.attribute.get_unchecked(i),
                data_oriented.word_index.get_unchecked(i),
                data_oriented.is_exact.get_unchecked(i),
            )
        });

        apply_permutations(&permutations, &mut data_oriented.query_index);
        apply_permutations(&permutations, &mut data_oriented.distance);
        apply_permutations(&permutations, &mut data_oriented.attribute);
        apply_permutations(&permutations, &mut data_oriented.word_index);
        apply_permutations(&permutations, &mut data_oriented.is_exact);

        // after sort
        for i in 0..length {
            let classic = &classics[i];
            assert_eq!(classic.query_index, data_oriented.query_index[i]);
            assert_eq!(classic.distance,    data_oriented.distance[i]);
            assert_eq!(classic.attribute,   data_oriented.attribute[i]);
            assert_eq!(classic.word_index,  data_oriented.word_index[i]);
            assert_eq!(classic.is_exact,    data_oriented.is_exact[i]);
        }
    }
}

#[cfg(all(feature = "nightly", test))]
mod bench {
    extern crate test;

    use super::*;

    use rand::SeedableRng;
    use rand::rngs::StdRng;

    #[bench]
    fn classics_16_000(b: &mut test::Bencher) {
        let rng = StdRng::from_seed([42; 32]);
        let data = new_classics(rng, 16_000);

        b.iter(|| {
            data.clone().sort_unstable();
        })
    }

    #[bench]
    fn data_oriented_16_000(b: &mut test::Bencher) {
        let rng = StdRng::from_seed([42; 32]);
        let data = DataOriented::new(rng, 16_000);

        b.iter(|| {
            let mut data = data.clone();
            let permutations = permutations_unstable_by_key(data.len(), |i| unsafe {
                (
                    data.query_index.get_unchecked(i),
                    data.distance.get_unchecked(i),
                    data.attribute.get_unchecked(i),
                    data.word_index.get_unchecked(i),
                    data.is_exact.get_unchecked(i),
                )
            });

            apply_permutations(&permutations, &mut data.query_index);
            apply_permutations(&permutations, &mut data.distance);
            apply_permutations(&permutations, &mut data.attribute);
            apply_permutations(&permutations, &mut data.word_index);
            apply_permutations(&permutations, &mut data.is_exact);
        })
    }
}

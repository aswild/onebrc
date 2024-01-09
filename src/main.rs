use std::fmt;
use std::fs::File;

use bstr::{BStr, BString, ByteSlice};
use memmap2::Mmap;

mod temperature;
use temperature::Temperature;

type HashMap<K, V> = ahash::AHashMap<K, V>;

#[derive(Debug, Clone, Copy)]
struct Row<'a> {
    city: &'a BStr,
    temp: Temperature,
}

impl<'a> Row<'a> {
    /// Parse a single row. SPICY HOT!
    fn parse(s: &'a BStr) -> Option<Self> {
        // split at the location of the ';'. This means the first character of what we send to
        // Temperature::parse is ';' but that's fine, it'll be ignored there (and saves us
        // extra bounds checks manually slicing that away here).
        // And since lines are short (only a few dozen bytes) it's faster to use a basic naive
        // linear byte-by-byte search that s.iter().position() compiles down to rather than
        // something like memchr.
        let (city, temp_s) = s.split_at(s.iter().position(|b| *b == b';')?);
        let temp = Temperature::parse(temp_s);
        Some(Self {
            city: BStr::new(city),
            temp,
        })
    }
}

#[derive(Debug, Clone, Copy)]
struct Stats {
    total: Temperature,
    count: u32,
    min: Temperature,
    max: Temperature,
}

#[derive(Debug, Clone, Copy)]
struct FinalStats {
    mean: Temperature,
    min: Temperature,
    max: Temperature,
}

impl fmt::Display for FinalStats {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}/{}/{}", self.min, self.mean, self.max)
    }
}

impl Stats {
    fn new(temp: Temperature) -> Self {
        Self {
            total: temp,
            count: 1,
            min: temp,
            max: temp,
        }
    }

    fn finalize(self) -> FinalStats {
        FinalStats {
            mean: self.total / self.count,
            min: self.min,
            max: self.max,
        }
    }

    fn update_row(&mut self, temp: Temperature) {
        self.total += temp;
        self.count += 1;
        if temp < self.min {
            self.min = temp;
        }
        if temp > self.max {
            self.max = temp;
        }
    }

    fn update_stats(&mut self, other: Stats) {
        self.total += other.total;
        self.count += other.count;
        if other.min < self.min {
            self.min = other.min;
        }
        if other.max > self.max {
            self.max = other.max;
        }
    }
}

#[derive(Debug, Default)]
struct ResultsMap {
    map: HashMap<BString, Stats>,
}

impl ResultsMap {
    /// add a single row to these results
    fn ingest(&mut self, row: Row) {
        if let Some(stats) = self.map.get_mut(row.city) {
            stats.update_row(row.temp);
        } else {
            self.map.insert(row.city.into(), Stats::new(row.temp));
        }
    }

    /// combine with all of `other`'s results
    fn merge(&mut self, other: ResultsMap) {
        // special case if we're merging into an empty map, we can just assume the other map
        // in-place
        if self.map.is_empty() {
            *self = other;
            return;
        }

        for (city, stats) in other {
            if let Some(my_stats) = self.map.get_mut(&city) {
                my_stats.update_stats(stats);
            } else {
                self.map.insert(city, stats);
            }
        }
    }
}

impl std::ops::Add for ResultsMap {
    type Output = Self;

    fn add(mut self, rhs: Self) -> Self::Output {
        self.merge(rhs);
        self
    }
}

impl std::iter::Sum for ResultsMap {
    fn sum<I: Iterator<Item = Self>>(mut iter: I) -> Self {
        let first = match iter.next() {
            Some(x) => x,
            None => return Self::default(),
        };
        iter.fold(first, std::ops::Add::add)
    }
}

impl IntoIterator for ResultsMap {
    type Item = (BString, Stats);
    type IntoIter = <HashMap<BString, Stats> as IntoIterator>::IntoIter;

    fn into_iter(self) -> Self::IntoIter {
        self.map.into_iter()
    }
}

/// Given a buffer containing input file contents (possibly mmap'd), collect all of the
/// measurement results together.
///
/// This is the meat of the work, the vast majority of program runtime is spent in this function.
/// It's not inlined for better visibility in perf tools, even though it's only called once from
/// main().
#[cfg(feature = "rayon")]
#[inline(never)]
fn process_data(data: &[u8]) -> ResultsMap {
    use rayon::prelude::*;

    // split on lines in parallel
    data.par_split(|b| *b == b'\n')
        // Rayon will make a bunch of ResultsMaps (the exact amount isn't specified beyond "as
        // needed" but I've seen it surpass 25,000) and reuse them whenever it calls this closure
        // in a worker thread. fold() returns a ParallelIterator<Item = ResultsMap>.
        .fold(ResultsMap::default, |mut results, line| {
            // SPICY HOT! Called for every line.
            if let Some(row) = Row::parse(line.as_bstr()) {
                results.ingest(row);
            }
            // pass on results accumulator for next task
            results
        })
        // Then immediately (and still in parallel) reduce those ResultsMaps into a single one.
        // Somehow this (which uses the std::iter::Sum impl above) is faster than using
        // ParallelIterator::reduce, even though it's basically the same code.
        .sum()
}

/// Single-threaded version of the above
#[cfg(not(feature = "rayon"))]
#[inline(never)]
fn process_data(data: &[u8]) -> ResultsMap {
    data.split(|&b| b == b'\n')
        .fold(ResultsMap::default(), |mut results, line| {
            // SPICY HOT! Called for every line.
            if let Some(row) = Row::parse(line.as_bstr()) {
                results.ingest(row);
            }
            // pass on results accumulator for next task
            results
        })
}

fn main() {
    let measurements_path = std::env::args().nth(1).expect("missing filename argument");
    let file = File::open(measurements_path).expect("failed to open input file");

    // mmap the whole thing, accessible as a bug &[u8]. No UTF-8 check
    let data = unsafe { Mmap::map(&file).expect("failed to mmap input file") };

    // do all the main work
    let merged_results = process_data(&data);

    // Finalize statstics: determine the mean temperatures and sort by city name. It's faster to do
    // this serially, since rayon's parallel iteration over maps is to first collect them into an
    // intermediate Vec, and the computation in stats.finalize is cheap (like 3 f64 ops).
    let mut summary_results: Vec<(BString, FinalStats)> = merged_results
        .into_iter()
        .map(|(city, stats)| (city, stats.finalize()))
        .collect();
    summary_results.sort_unstable_by(|a, b| a.0.cmp(&b.0));

    // Print results
    print!("{{");
    for (i, (city, stats)) in summary_results.into_iter().enumerate() {
        let comma = if i == 0 { "" } else { ", " };
        print!("{comma}{city}={stats}");
    }
    println!("}}");
}

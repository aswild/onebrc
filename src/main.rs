use std::fmt;
use std::fs::File;

use bstr::{BStr, BString, ByteSlice};
use memmap2::Mmap;
use rayon::prelude::*;

mod temperature;
use temperature::Temperature;

type HashMap<K, V> = ahash::AHashMap<K, V>;

#[derive(Debug, Clone, Copy)]
struct Row<'a> {
    city: &'a BStr,
    temp: Temperature,
}

impl<'a> Row<'a> {
    fn parse(s: &'a BStr) -> Self {
        match Self::try_parse(s) {
            Ok(row) => row,
            Err(err) => panic!("Failed to parse row '{s}': {err}"),
        }
    }

    fn try_parse(s: &'a BStr) -> Result<Self, &'static str> {
        // apparently we don't have split_once stable for slices yet, and the input lines aren't
        // big enough to drag in memchr, so just roll my own. Interestingly, using iterators and
        // position are the most "idiomatic" ways to implement "find index of first matching
        // element" in a slice. I can't believe there's not a slice::find method or similar.
        let (city, temp_s) = {
            let index = s.iter().position(|&b| b == b';').ok_or("missing ';'")?;
            (&s[..index], &s[(index + 1)..])
        };

        let temp = Temperature::parse(temp_s).map_err(|_| "failed to parse number")?;
        Ok(Self {
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

fn main() {
    let measurements_path = std::env::args().nth(1).expect("missing filename argument");
    let file = File::open(measurements_path).expect("failed to open input file");

    // mmap the whole thing, accessible as a bug &[u8]. No UTF-8 check
    let data = unsafe { Mmap::map(&file).expect("failed to mmap input file") };

    // HOT CODE: this is the main computation loop. Rayon will make a bunch of ResultsMaps
    // (I think when dispatching jobs on new workers, but the exact logic isn't specified beyond
    // "as needed". On a computer with a lot of cores, this can go up to a few tens of thousands of
    // intermediate maps).
    let merged_results: ResultsMap = data
        .par_split(|b| *b == b'\n')
        .fold(ResultsMap::default, |mut results, line| {
            // Main worker task. Check for empty lines, such as encountered at EOF
            if !line.is_empty() {
                let row = Row::parse(line.as_bstr());
                results.ingest(row);
            }
            // pass on results accumulator for next task
            results
        })
        // Then immediately (and still in parallel) reduce those ResultsMaps into a single one.
        // Somehow this, combined with the std::iter::Sum impl above, is faster than using
        // ParallelIterator::reduce here, even though it's basically the same code.
        .sum();
    //.reduce(ResultsMap::default, |mut acc, e| {
    //    acc.merge(e);
    //    acc
    //});

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

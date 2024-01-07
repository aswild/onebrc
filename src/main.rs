use std::fmt;
use std::fs::File;

use memmap2::Mmap;
use rayon::prelude::*;

type HashMap<K, V> = std::collections::HashMap<K, V>;

#[derive(Debug, Clone, Copy)]
struct Row<'a> {
    city: &'a str,
    temp: f32,
}

impl<'a> Row<'a> {
    fn parse(s: &'a str) -> Self {
        match Self::try_parse(s) {
            Ok(row) => row,
            Err(err) => panic!("Failed to parse row '{s}': {err}"),
        }
    }

    fn try_parse(s: &'a str) -> Result<Self, &'static str> {
        let (city, s) = s.split_once(';').ok_or("missing ';' in line")?;
        let temp = s.parse::<f32>().map_err(|_| "failed to parse number")?;
        Ok(Self { city, temp })
    }
}

#[derive(Debug, Clone, Copy)]
struct Stats {
    total: f32,
    count: u32,
    min: f32,
    max: f32,
}

#[derive(Debug, Clone, Copy)]
struct FinalStats {
    mean: f32,
    min: f32,
    max: f32,
}

impl fmt::Display for FinalStats {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:.1}/{:.1}/{:.1}", self.min, self.mean, self.max)
    }
}

impl Stats {
    fn new(temp: f32) -> Self {
        Self {
            total: temp,
            count: 1,
            min: temp,
            max: temp,
        }
    }

    fn finalize(self) -> FinalStats {
        let mean = self.total / (self.count as f32);
        // round to one decimal place, which we have to do as an actual round operation rather than
        // as part of output formatting, otherwise some 1.X5 values round down instead of up which
        // doesn't match the correct output.
        let mean = (mean * 10.0).round() / 10.0;

        FinalStats {
            mean,
            min: self.min,
            max: self.max,
        }
    }

    fn update_row(&mut self, temp: f32) {
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
    map: HashMap<String, Stats>,
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
        for (city, stats) in other {
            if let Some(my_stats) = self.map.get_mut(&city) {
                my_stats.update_stats(stats);
            } else {
                self.map.insert(city, stats);
            }
        }
    }
}

impl IntoIterator for ResultsMap {
    type Item = (String, Stats);
    type IntoIter = <HashMap<String, Stats> as IntoIterator>::IntoIter;

    fn into_iter(self) -> Self::IntoIter {
        self.map.into_iter()
    }
}

fn main() {
    let measurements_path = std::env::args().nth(1).expect("missing filename argument");
    let file = File::open(measurements_path).expect("failed to open input file");

    // mmap the whole thing and cast to a string (do a huge utf-8 validation)
    let data = unsafe { Mmap::map(&file).expect("failed to mmap input file") };
    let data_str = std::str::from_utf8(&data).expect("input file is not UTF-8");

    let merged_results: ResultsMap = data_str
        .par_lines()
        // Fold lines into a collection of ResultsMaps (nominally one per worker thread). The
        // closure is given the accumulator by value, and returns the new accumulator. Continues
        // the ParallelIterator where Item = ResultsMap
        .fold(ResultsMap::default, |mut results, line| {
            let row = Row::parse(line);
            results.ingest(row);
            results
        })
        // reduce all of the ResultsMaps together into one
        .reduce(ResultsMap::default, |mut acc, e| {
            acc.merge(e);
            acc
        });

    let mut summary_results: Vec<(String, FinalStats)> = merged_results
        .into_iter()
        .map(|(city, stats)| (city, stats.finalize()))
        .collect();
    summary_results.sort_unstable_by(|a, b| a.0.cmp(&b.0));

    print!("{{");
    for (i, (city, stats)) in summary_results.into_iter().enumerate() {
        let comma = if i == 0 { "" } else { ", " };
        print!("{comma}{city}={stats}");
    }
    println!("}}");
}

use std::fmt;
use std::fs::File;
use std::io::{BufRead, BufReader};

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

    #[allow(unused)]
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
    map: std::collections::HashMap<String, Stats>,
}

impl ResultsMap {
    fn ingest(&mut self, row: Row) {
        if let Some(stats) = self.map.get_mut(row.city) {
            stats.update_row(row.temp);
        } else {
            self.map.insert(row.city.into(), Stats::new(row.temp));
        }
    }
}

fn main() {
    let measurements_path = std::env::args().nth(1).expect("missing filename argument");
    let mut file =
        BufReader::new(File::open(measurements_path).expect("failed to open input file"));

    let mut line = String::new();
    let mut results = ResultsMap::default();

    loop {
        line.clear();
        match file.read_line(&mut line) {
            Ok(0) => break,
            Ok(_) => (),
            Err(err) => panic!("failed to read line: {err}"),
        }

        if line.ends_with('\n') {
            line.pop();
        }
        let row = Row::parse(&line);
        results.ingest(row);
    }

    let mut final_results: Vec<(String, FinalStats)> = results
        .map
        .into_iter()
        .map(|(city, stats)| (city, stats.finalize()))
        .collect();
    final_results.sort_unstable_by(|a, b| a.0.cmp(&b.0));

    print!("{{");
    for (i, (city, stats)) in final_results.into_iter().enumerate() {
        let comma = if i == 0 { "" } else { ", " };
        print!("{comma}{city}={stats}");
    }
    println!("}}");
}

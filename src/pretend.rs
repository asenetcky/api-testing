use std::fs::File;
use std::io::{BufRead, BufReader};

// // pretend this is a reqwest to future api
pub fn read_lines(filename: &str) -> std::io::Result<Vec<String>> {
    let file = File::open(filename)?;
    let reader = BufReader::new(file);
    let mut lines = Vec::new();

    for line in reader.lines() {
        match line {
            Ok(content) => lines.push(content),
            Err(e) => eprintln!("Error reading line: {}", e),
        }
    }
    lines.sort();
    lines.dedup();
    Ok(lines)
}

use polars::prelude::*;

/// Display a slice of Option<DataFrame> with a given label.
pub fn display(results: &[Option<DataFrame>], label: &str) {
    for (i, df_opt) in results.iter().enumerate() {
        match df_opt {
            Some(df) => {
                println!("{} {}:\n{}", label, i + 1, df);
            }
            None => {
                println!("{} {}: Failed to fetch or parse.", label, i + 1);
            }
        }
    }
}

pub async fn fetch_labels(urls: &[String], label: &str) -> Result<(), Box<dyn std::error::Error>> {
    let results = crate::urls::get_census(urls).await;
    display(&results, label);
    Ok(())
}

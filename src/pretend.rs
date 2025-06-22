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
pub fn display(results: &[DataFrame], label: &str) {
    for (i, df) in results.iter().enumerate() {
        println!("{} {}:\n{}", label, i + 1, df);
    }
}

pub async fn fetch_labels(urls: &[String], label: &str) -> Result<(), Box<dyn std::error::Error>> {
    let results = crate::urls::fetch_all_variable_labels(urls).await;
    let dfs: Vec<_> = results.into_iter().filter_map(|opt| opt).collect();
    display(&dfs, label);
    Ok(())
}

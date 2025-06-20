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
    Ok(lines)
}

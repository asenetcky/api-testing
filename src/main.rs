use polars::prelude::*;
use std::fs::File;
use std::io::*;
use url::Url;

fn main() {
    playground();

    read_lines("census.txt")
        .unwrap()
        .iter()
        .for_each(|line| println!("{}", line));

    parse_url();
}

// example url parse
fn parse_url() {
    let url: &'static str =
        "https://api.census.gov/data/2023/acs/acs1?get=group(B05006)&ucgid=0400000US09";
    let parsed_url: Url = Url::parse(url).unwrap();

    println!("Scheme: {}", parsed_url.scheme());
    println!("Host: {}", parsed_url.host_str().unwrap_or(""));
    println!("Path: {}", parsed_url.path());
    println!("Query: {}", parsed_url.query().unwrap_or(""));
    println!("Fragment: {}", parsed_url.fragment().unwrap_or(""));
}

fn playground() {
    let mut file: File = std::fs::File::open("storage-format.parquet").unwrap();

    let df: DataFrame = ParquetReader::new(&mut file).finish().unwrap();
    println!("{}", df);
    println!(
        "{:?}",
        df.drop_many(["row_id", "dataset", "year", "geo_id",])
    );
}

// pretend this is a reqwest to future api
fn read_lines(filename: &str) -> std::io::Result<Vec<String>> {
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

use polars::io::json;
use polars::prelude::*;
use reqwest::*;
use serde_json::*;
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
    println!("Parsed URL: {}", parsed_url);
    println!("Scheme: {}", parsed_url.scheme());
    println!("Host: {}", parsed_url.host_str().unwrap_or(""));
    println!("Path: {}", parsed_url.path());
    println!("Query: {}", parsed_url.query().unwrap_or(""));
    println!("Fragment: {}", parsed_url.fragment().unwrap_or(""));
    println!("Domain: {}", parsed_url.domain().unwrap_or(""));
    println!("Username: {}", parsed_url.username());
    println!("Password: {}", parsed_url.password().unwrap_or(""));
    println!(
        "Path Segments: {}",
        parsed_url
            .path_segments()
            .unwrap()
            .collect::<Vec<_>>()
            .join("/")
    );

    let mut query_pairs = parsed_url.query_pairs();
    while let Some((key, value)) = query_pairs.next() {
        println!("{}: {}", key, value);
    }
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

fn pull_data(url: Url) -> DataFrame {
    let client = reqwest::Client::new();
    let response = client.get(url).send();
    let json_data = match response {
        Ok(resp) => resp.text().unwrap(),
        Err(e) => {
            eprintln!("Error fetching data: {}", e);
            return DataFrame::default();
        }
    };
    let df = json::from_str(&json_data).unwrap();
    println!("DataFrame: {}", df);
    df
}

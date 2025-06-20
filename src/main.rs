use futures::future::join_all;
use polars::prelude::*;
use reqwest;
use serde_json::Value;
use std::fs::File;
use std::io::{BufRead, BufReader};
use tokio;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // let urls = vec![
    //     "https://api.census.gov/data/2023/acs/acs1?get=group(B05006)&ucgid=0400000US09",
    //     "https://api.census.gov/data/2023/acs/acs5?get=group(B05006)&ucgid=0400000US09",
    //     "https://api.census.gov/data/2021/acs/acs5?get=group(B05006)&ucgid=0400000US09",
    //     "https://api.census.gov/data/2021/acs/acs1?get=group(B05006)&ucgid=0400000US09",
    // ];

    let urls = read_lines("census.txt").unwrap();

    let client = reqwest::Client::new();

    let futures = urls.iter().map(|url| pull_data(&client, url));
    let results: Vec<Option<DataFrame>> = join_all(futures).await;

    for (i, df_opt) in results.into_iter().enumerate() {
        match df_opt {
            Some(df) => println!("DataFrame {}:\n{}", i + 1, df),
            None => println!("DataFrame {}: Failed to fetch or parse.", i + 1),
        }
    }

    Ok(())

    // let resp = reqwest::get(
    //     "https://api.census.gov/data/2023/acs/acs1?get=group(B05006)&ucgid=0400000US09",
    // )
    // .await?;

    // let body = resp.text().await?;

    // let json: Value = serde_json::from_str(&body).expect("Failed to parse JSON");

    // //    println!("{json:#?}");
    // let is_array = json.is_array();
    // println!("{is_array}");

    // let slice0 = &json[0];
    // let slice1 = &json[1..];

    // println!("Slice: {slice0:#?}");
    // println!("Slice: {slice1:#?}");
    // Ok(())
}

async fn pull_data(client: &reqwest::Client, url: &str) -> Option<DataFrame> {
    let resp = client.get(url).send().await.ok()?;
    let body = resp.text().await.ok()?;
    let json: Value = serde_json::from_str(&body).ok()?;
    let is_array = json.is_array();
    println!("{is_array}");

    // Expecting the JSON to be an array of arrays, where the first element is the header
    // and the rest are data rows
    let arr = json.as_array()?;
    if arr.len() < 2 {
        return None;
    }
    let headers = arr[0].as_array()?;
    let data_rows = &arr[1..];

    // Transpose rows to columns
    // let mut columns: Vec<Vec<String>> = vec![Vec::new(); headers.len()];
    // for row in data_rows {
    //     let row_arr = row.as_array()?;
    //     for (i, cell) in row_arr.iter().enumerate() {
    //         columns[i].push(cell.as_str().unwrap_or("").to_string());
    //     }
    // }

    // --- Wide format code for reference ---
    // // Build Column for each header/column pair
    // let columns: Vec<Column> = headers
    //     .iter()
    //     .zip(columns.into_iter())
    //     .map(|(header, col)| {
    //         let name = header.as_str().unwrap_or("unknown").to_string();
    //         // Create a Series, then convert to Column
    //         Series::new(name.into(), col).into_column()
    //     })
    //     .collect();
    // let df = DataFrame::new(columns).ok()?;

    // Reshape to long format: columns "header" and "value"
    let mut header_col = Vec::new();
    let mut value_col = Vec::new();

    for row in data_rows {
        let row_arr = row.as_array()?;
        for (i, cell) in row_arr.iter().enumerate() {
            let header = headers[i].as_str().unwrap_or("unknown");
            let value = cell.as_str().unwrap_or("").to_string();
            header_col.push(header.to_string());
            value_col.push(value);
        }
    }

    let header_series = Series::new("header".into(), header_col).into_column();
    let value_series = Series::new("value".into(), value_col).into_column();

    let df = DataFrame::new(vec![header_series, value_series]).ok()?;

    Some(df)
}

// TODO: capture as much metadata as possible from the url
// store in struct
// async fetch all the data - wrangle with polars

// example url parse
// fn parse_url() {
//     let url: &'static str =
//         "https://api.census.gov/data/2023/acs/acs1?get=group(B05006)&ucgid=0400000US09";
//     let parsed_url: Url = Url::parse(url).unwrap();
//     println!("Parsed URL: {}", parsed_url);
//     println!("Scheme: {}", parsed_url.scheme());
//     println!("Host: {}", parsed_url.host_str().unwrap_or(""));
//     println!("Path: {}", parsed_url.path());
//     println!("Query: {}", parsed_url.query().unwrap_or(""));
//     println!("Fragment: {}", parsed_url.fragment().unwrap_or(""));
//     println!("Domain: {}", parsed_url.domain().unwrap_or(""));
//     println!("Username: {}", parsed_url.username());
//     println!("Password: {}", parsed_url.password().unwrap_or(""));
//     println!(
//         "Path Segments: {}",
//         parsed_url
//             .path_segments()
//             .unwrap()
//             .collect::<Vec<_>>()
//             .join("/")
//     );

//     let mut query_pairs = parsed_url.query_pairs();
//     while let Some((key, value)) = query_pairs.next() {
//         println!("{}: {}", key, value);
//     }
// }

// fn playground() {
//     let mut file: File = std::fs::File::open("storage-format.parquet").unwrap();

//     let df: DataFrame = ParquetReader::new(&mut file).finish().unwrap();
//     println!("{}", df);
//     println!(
//         "{:?}",
//         df.drop_many(["row_id", "dataset", "year", "geo_id",])
//     );
// }

// // pretend this is a reqwest to future api
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

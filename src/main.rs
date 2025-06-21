use crate::run::*;

pub mod acs;
pub mod data;
pub mod pretend;
pub mod run;
pub mod url;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    if let Err(e) = run().await {
        eprintln!("{e}");
        std::process::exit(1);
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

use futures::future::join_all;
use polars::prelude::*;
use reqwest;

use crate::acs;
use crate::data::fetch_geo_dataframe;
use crate::pretend;

pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let urls = pretend::read_lines("census.txt")?;

    let client = reqwest::Client::new();

    let futures = urls.iter().map(|url| acs::pull_data(&client, url));
    let results: Vec<Option<DataFrame>> = join_all(futures).await;

    for (i, df_opt) in results.clone().into_iter().enumerate() {
        match df_opt {
            Some(df) => println!("DataFrame {}:\n{}", i + 1, df),
            None => println!("DataFrame {}: Failed to fetch or parse.", i + 1),
        }
    }
    // let geo = &results[1].clone();
    for (i, geo_opt) in results.clone().into_iter().enumerate() {
        match geo_opt {
            Some(geo) => println!("geo {}:\n{}", i + 1, fetch_geo_dataframe(geo)),
            None => println!("geo {}: Failed to fetch or parse.", i + 1),
        }
    }
    Ok(())
}

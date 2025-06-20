use futures::future::join_all;
use polars::prelude::*;
use reqwest;

use crate::acs;
use crate::pretend;

pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let urls = pretend::read_lines("census.txt")?;

    let client = reqwest::Client::new();

    let futures = urls.iter().map(|url| acs::pull_data(&client, url));
    let results: Vec<Option<DataFrame>> = join_all(futures).await;

    for (i, df_opt) in results.into_iter().enumerate() {
        match df_opt {
            Some(df) => println!("DataFrame {}:\n{}", i + 1, df),
            None => println!("DataFrame {}: Failed to fetch or parse.", i + 1),
        }
    }
    Ok(())
}

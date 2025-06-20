use futures::future::join_all;
use polars::prelude::*;
use reqwest;

use crate::acs;
use crate::data::{fetch_geo_dataframe, filter_main_dataframe, join_and_fill_geo_dfs};
use crate::pretend;

pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let urls = pretend::read_lines("census.txt")?;

    let client = reqwest::Client::new();

    let futures = urls.iter().map(|url| acs::pull_data(&client, url));
    let results: Vec<Option<DataFrame>> = join_all(futures).await;

    // Separate geo and main dataframes
    let mut geo_dfs = Vec::new();
    let mut main_dfs = Vec::new();

    for (i, df_opt) in results.into_iter().enumerate() {
        match df_opt {
            Some(df) => {
                // Extract geo dataframe
                let geo_df = fetch_geo_dataframe(df.clone());
                geo_dfs.push(geo_df);

                // Filter out geo rows from main dataframe
                let main_df = filter_main_dataframe(&df);
                main_dfs.push(main_df);

                println!("Main DataFrame {}:\n{}", i + 1, main_dfs.last().unwrap());
                println!("Geo DataFrame {}:\n{}", i + 1, geo_dfs.last().unwrap());
            }
            None => {
                println!("DataFrame {}: Failed to fetch or parse.", i + 1);
            }
        }
    }

    // Join and forward-fill geo dataframes horizontally
    // let geo_joined_filled = join_and_fill_geo_dfs(&geo_dfs);
    // if geo_joined_filled.width() > 0 {
    //     println!(
    //         "Joined and forward-filled geo DataFrame:\n{}",
    //         geo_joined_filled
    //     );
    // }

    Ok(())
}

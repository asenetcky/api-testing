use futures::future::join_all;
use polars::prelude::*;
use reqwest;

use crate::acs;
use crate::data::{fetch_geo_dataframe, filter_main_dataframe};
use crate::pretend;
use crate::urls;

pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let pretend = pretend::read_lines("census.txt").expect("cannot read pretend lines");
    let results = urls::get_census(&pretend).await;

    // Separate geo and main dataframes
    use crate::data::{fetch_geo_dataframe, filter_main_dataframe};

    let geo_dfs: Vec<_> = results
        .iter()
        .map(|df_opt| df_opt.as_ref().map(|df| fetch_geo_dataframe(df.clone())))
        .collect();

    let main_dfs: Vec<_> = results
        .iter()
        .map(|df_opt| df_opt.as_ref().map(|df| filter_main_dataframe(df)))
        .collect();

    // Display results using generic pretend::display for iterative development
    pretend::display(&main_dfs, "Main DataFrame");
    pretend::display(&geo_dfs, "Geo DataFrame");

    // Example: fetch and display variable labels using pretend::fetch_labels
    let var_urls = vec![crate::PLACEHOLDER_VAR_URL.to_string()];
    pretend::fetch_labels(&var_urls, "Variable Labels").await?;

    Ok(())
}

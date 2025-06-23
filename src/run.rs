use futures::future::join_all;
use polars::prelude::*;
use reqwest;

use crate::data::{fetch_geo_dataframe, filter_main_dataframe};
use crate::pretend;
use crate::urls::{self, fetch_relevant_variable_labels};

pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let pretend = pretend::read_lines("census.txt").expect("cannot read pretend lines");

    let merged = fetch_relevant_variable_labels(&pretend).await;
    pretend::display(&merged, "Merged DataFrame");
    // Use get_census_data for main/geo endpoints
    // let results = urls::get_census_data(&pretend).await;

    // use crate::data::{fetch_geo_dataframe, filter_main_dataframe};

    // let geo_dfs: Vec<DataFrame> = results
    //     .iter()
    //     .filter_map(|df_opt| df_opt.as_ref().map(|df| fetch_geo_dataframe(df.clone())))
    //     .collect();

    // let main_dfs: Vec<DataFrame> = results
    //     .iter()
    //     .filter_map(|df_opt| df_opt.as_ref().map(|df| filter_main_dataframe(df)))
    //     .collect();

    // pretend::display(&main_dfs, "Main DataFrame");
    // pretend::display(&geo_dfs, "Geo DataFrame");

    // // Use get_census_variables for variable endpoints
    // let var_urls = vec![crate::PLACEHOLDER_VAR_URL.to_string()];
    // let var_results = urls::fetch_all_variable_labels(&var_urls).await;
    // let var_dfs: Vec<DataFrame> = var_results.into_iter().filter_map(|opt| opt).collect();
    // pretend::display(&var_dfs, "Variables DataFrame");

    Ok(())
}

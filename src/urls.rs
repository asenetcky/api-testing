use futures::future::join_all;

use crate::{
    acs,
    data::{fetch_geo_dataframe, filter_main_dataframe},
};

use polars::{functions::concat_df_horizontal, prelude::*};
use reqwest;
use std::collections::HashMap;
use url::Url;

#[allow(dead_code)]
pub struct CensusAPIEndpoint {
    base_url: reqwest::Url,
    year: u8,
    dataset: DataFrame,
    variables: DataFrame,
    geography: HashMap<String, String>,
    api_key: String,
}

#[allow(dead_code)]
impl CensusAPIEndpoint {
    fn new(
        base_url: reqwest::Url,
        year: u8,
        dataset: DataFrame,
        variables: DataFrame,
        geography: HashMap<String, String>,
        api_key: String,
    ) -> CensusAPIEndpoint {
        CensusAPIEndpoint {
            base_url,
            year,
            dataset,
            variables,
            geography,
            api_key,
        }
    }

    // fn from_url(url: &str) -> CensusAPIEndpoint {
    //     let parsed_url = Url::parse(url).expect("cannot parse url.");
    //     quary_params = &parsed_url.query_pairs().unwrap_or("");

    // }
}

/// For endpoints that return main/geo data (with "header" column)
pub async fn get_census_data(urls: &[String]) -> Vec<Option<DataFrame>> {
    let client = reqwest::Client::new();

    let futures = urls.iter().map(|url| acs::pull_data(&client, url.as_str()));
    let results: Vec<Option<DataFrame>> = join_all(futures).await;
    results
}

/// For endpoints that return variable data (with "name", "label", "concept" columns)
pub async fn fetch_all_variable_labels(urls: &[String]) -> Vec<Option<DataFrame>> {
    let client = reqwest::Client::new();

    let futures = urls
        .iter()
        .map(|url| acs::pull_variables(&client, url.as_str()));
    let results: Vec<Option<DataFrame>> = join_all(futures).await;
    results
}

pub async fn fetch_relevant_variable_labels(urls: &[String]) -> Vec<DataFrame> {
    let var_labels = fetch_all_variable_labels(urls).await;
    let results = get_census_data(urls).await;

    let geos: Vec<DataFrame> = results
        .iter()
        .filter_map(|df_opt| df_opt.as_ref().map(|df| fetch_geo_dataframe(df.clone())))
        .collect();

    let dfs: Vec<DataFrame> = results
        .iter()
        .filter_map(|df_opt| df_opt.as_ref().map(|df| filter_main_dataframe(df)))
        .collect();

    let mut merged = Vec::new();

    for (df, geo) in dfs.into_iter().zip(geos.into_iter()) {
        let concat = concat_df_horizontal(&[df, geo], false).unwrap();
        // Forward fill nulls for all columns
        let filled_columns: Vec<_> = concat
            .get_columns()
            .iter()
            .map(|col| {
                col.as_materialized_series()
                    .fill_null(polars::prelude::FillNullStrategy::Forward(None))
                    .unwrap()
                    .into_column()
            })
            .collect();
        let filled_df = DataFrame::new(filled_columns).unwrap();
        merged.push(filled_df);
    }

    merged

    //TODO: now left_join on the variable labels etc...
}

use futures::future::join_all;

use crate::acs;

use polars::prelude::*;
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

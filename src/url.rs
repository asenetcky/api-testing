use polars::prelude::*;
use reqwest;
use std::collections::HashMap;
use url::Url;

pub struct CensusAPIEndpoint {
    base_url: reqwest::Url,
    year: u8,
    dataset: DataFrame,
    variables: DataFrame,
    geography: HashMap<String, String>,
    api_key: String,
}

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

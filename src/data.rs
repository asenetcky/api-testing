use polars::prelude::*;
use std::collections::HashMap;

use crate::PLACEHOLDER_VAR_URL;
use crate::urls;

/// Extract the geo dataframe (always geo_id, ucgid, geo_name columns, missing as None)
pub fn fetch_geo_dataframe(df: DataFrame) -> DataFrame {
    let filtered = df
        .clone()
        .lazy()
        .with_column(col("header").str().to_lowercase())
        .filter(
            col("header")
                .eq(lit("geo_id"))
                .or(col("header").eq(lit("name")))
                .or(col("header").eq(lit("ucgid"))),
        )
        .with_column(
            when(col("header").eq(lit("name")))
                .then(lit("geo_name"))
                .otherwise(col("header"))
                .alias("header"),
        )
        .select([col("header"), col("value")])
        .collect()
        .expect("could not parse dataframe");

    let header_vals: HashMap<String, Option<String>> = {
        let headers_series = filtered
            .column("header")
            .expect("header col")
            .as_materialized_series();
        let values_series = filtered
            .column("value")
            .expect("value col")
            .as_materialized_series();

        let headers_vec: Vec<Option<&str>> = headers_series
            .str()
            .expect("header str")
            .as_ref()
            .into_iter()
            .collect();
        let values_vec: Vec<Option<&str>> = values_series
            .str()
            .expect("value str")
            .as_ref()
            .into_iter()
            .collect();

        headers_vec
            .into_iter()
            .zip(values_vec.into_iter())
            .filter_map(|(h, v)| h.map(|hh| (hh.to_string(), v.map(|s| s.to_string()))))
            .collect()
    };

    let col_names = ["geo_id", "ucgid", "geo_name"];
    let cols: Vec<Column> = col_names
        .iter()
        .map(|&col| {
            let val = header_vals.get(col).cloned().flatten();
            Series::new(col.into(), [val]).into_column()
        })
        .collect();

    DataFrame::new(cols).expect("could not build geo dataframe")
}

/// Filter out geo rows from a long-format DataFrame
pub fn filter_main_dataframe(df: &DataFrame) -> DataFrame {
    df.clone()
        .lazy()
        .filter(
            col("header")
                .str()
                .to_lowercase()
                .neq(lit("geo_id"))
                .and(col("header").str().to_lowercase().neq(lit("ucgid")))
                .and(col("header").str().to_lowercase().neq(lit("name"))),
        )
        .collect()
        .expect("could not filter main dataframe")
}

// going to some of this to the url impl eventually

use polars::prelude::*;

// for reference - long dataframe with header: str and  value: str

pub fn fetch_geo_dataframe(df: DataFrame) -> DataFrame {
    df.clone()
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
        .collect()
        .expect("could not parse dataframe")
        .transpose(None, None)
        .expect("could not transpose dataframe")
}

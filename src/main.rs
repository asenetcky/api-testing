use polars::prelude::*;
use std::fs::File;:walking

fn main() {
    playground()
}

fn playground() {
    let mut file: File = std::fs::File::open("storage-format.parquet").unwrap();

    let df: DataFrame = ParquetReader::new(&mut file).finish().unwrap();
    println!("{}", df);
    println!("{:?}", df.drop_many([
        "row_id",
        "dataset",
        "year",
        "geo_id",
    ])
);
}

use polars::prelude::*;


fn main() {
    playground()
}

fn playground() {
    let mut file = std::fs::File::open("storage-format.parquet").unwrap();

    let df = ParquetReader::new(&mut file).finish().unwrap();
    println!("{}", df);
    println!("{:?}", df.drop_many([
        "row_id",
        "dataset",
        "year",
        "geo_id",
    ])
);
}

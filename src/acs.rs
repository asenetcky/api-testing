use polars::prelude::*;
use serde_json::Value;

pub async fn pull_data(client: &reqwest::Client, url: &str) -> Option<DataFrame> {
    let resp = client.get(url).send().await.ok()?;
    let body = resp.text().await.ok()?;
    let json: Value = serde_json::from_str(&body).ok()?;
    let is_array = json.is_array();
    println!("{is_array}");

    // Expecting the JSON to be an array of arrays, where the first element is the header
    // and the rest are data rows
    let arr = json.as_array()?;
    if arr.len() < 2 {
        return None;
    }
    let headers = arr[0].as_array()?;
    let data_rows = &arr[1..];

    // Reshape to long format: columns "header" and "value"
    let mut header_col = Vec::new();
    let mut value_col = Vec::new();

    for row in data_rows {
        let row_arr = row.as_array()?;
        for (i, cell) in row_arr.iter().enumerate() {
            let header = headers[i].as_str().unwrap_or("unknown");
            let value = cell.as_str().unwrap_or("").to_string();
            header_col.push(header.to_string());
            value_col.push(value);
        }
    }

    let header_series = Series::new("header".into(), header_col).into_column();
    let value_series = Series::new("value".into(), value_col).into_column();

    let df = DataFrame::new(vec![header_series, value_series]).ok()?;

    Some(df)
}

pub async fn pull_variables(client: &reqwest::Client, url: &str) -> Option<DataFrame> {
    let resp = client.get(url).send().await.ok()?;
    let body = resp.text().await.ok()?;
    let json: Value = serde_json::from_str(&body).ok()?;
    let arr = json.as_array()?;
    if arr.len() < 2 {
        return None;
    }
    // The first element is the headers array
    let headers = arr[0].as_array()?;
    let header_names: Vec<String> = headers
        .iter()
        .map(|h| h.as_str().unwrap_or("unknown").to_string())
        .collect();

    // The rest are data rows
    let data_rows = &arr[1..];

    // For each header, collect a Vec<String> of values
    let mut columns: Vec<Vec<String>> = vec![Vec::new(); header_names.len()];
    for row in data_rows {
        let row_arr = row.as_array()?;
        for (i, cell) in row_arr.iter().enumerate() {
            columns[i].push(cell.as_str().unwrap_or("").to_string());
        }
    }

    // Build a Series for each column
    let series: Vec<Series> = header_names
        .iter()
        .zip(columns.into_iter())
        .map(|(name, col)| Series::new(name.into(), col))
        .collect();

    let columns: Vec<Column> = series.into_iter().map(|s| s.into_column()).collect();
    let df = DataFrame::new(columns).ok()?;
    Some(df)
}

// Transpose rows to columns
// let mut columns: Vec<Vec<String>> = vec![Vec::new(); headers.len()];
// for row in data_rows {
//     let row_arr = row.as_array()?;
//     for (i, cell) in row_arr.iter().enumerate() {
//         columns[i].push(cell.as_str().unwrap_or("").to_string());
//     }
// }

// --- Wide format code for reference ---
// // Build Column for each header/column pair
// let columns: Vec<Column> = headers
//     .iter()
//     .zip(columns.into_iter())
//     .map(|(header, col)| {
//         let name = header.as_str().unwrap_or("unknown").to_string();
//         // Create a Series, then convert to Column
//         Series::new(name.into(), col).into_column()
//     })
//     .collect();
// let df = DataFrame::new(columns).ok()?;

use serde_json;
use serde::Deserialize;
use std::collections::HashMap;
use chrono::{NaiveDateTime, NaiveDate};


type Dict = HashMap<NaiveDate, Vec<serde_json::Value>>;

#[derive(Deserialize, Debug)]
struct Record {
    timestamp: String,
    json: String,
}

fn process_file(csv_file_to_process: String) -> Result<Dict, Box<dyn std::error::Error>> {

    let mut result_dict: Dict = Dict::new(); 
    let mut reader = csv::Reader::from_path(csv_file_to_process)?;

    for result in reader.deserialize::<Record>(){
        let record = result?;
        let key = NaiveDateTime::parse_from_str(record.timestamp.as_str(), "%Y-%m-%d %H:%M:%S%.3f")?;
        let v = serde_json::from_str(&record.json)?;
        result_dict.entry(key.date()).or_insert(Vec::new()).push(v);              
    }

    Ok(result_dict)
}

fn main() {

    let impression_per_date = HashMap::from([
        (NaiveDate::from_ymd(2022, 2, 1), 250.0),
        (NaiveDate::from_ymd(2022, 2, 2), 250.0),
        (NaiveDate::from_ymd(2022, 2, 3), 250.0),
        (NaiveDate::from_ymd(2022, 2, 4), 250.0),
        (NaiveDate::from_ymd(2022, 2, 5), 465.0),
        (NaiveDate::from_ymd(2022, 2, 6), 250.0),
    ]);

    let result =  process_file("./logs-insights-results_2.csv".to_string());
    match result{
        Ok(winning_bids) => {
            for (date, json_list) in winning_bids.iter() {
                let sum:f64 = json_list.iter().map(|x| x["bid"]["price"].as_f64().unwrap()).sum::<f64>();
                let nb:f64 = json_list.len() as f64;
                let imp = impression_per_date.get(date).unwrap_or(&1.0);

                println!("Date: {}, Winnings #: {}, Sum(price): {:.2}, Average(price): {:.2} => Spend (250 imp) ~ {:.2}",date, nb, sum, sum/nb, sum/nb*imp/1000.0);
            }
        }
        Err(e) => { println!("Application error: {}", e); }
    }
}


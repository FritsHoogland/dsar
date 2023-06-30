use std::collections::BTreeMap;
use prometheus_parse::{Value, Sample};
use itertools::Itertools;
use log::*;

use crate::Statistic;

pub fn process_statistic(
    sample: &Sample,
    hostname: &str,
    statistics: &mut BTreeMap<(String, String, String, String), Statistic>,
)
{
    let value = match sample.value
    {
        // Value::Untyped is the old YugabyteDB prometheus-metrics type
        Value::Untyped(value) => value,
        // Value::Counter is the new YugabyteDB prometheus-metrics type
        Value::Counter(value) => value,
        _ => {
            panic!("{} value enum type should be Untyped or Counter!", sample.metric);
        },
    };
    //let Value::Untyped(value) = sample.value else { panic!("{} value enum type should be Untyped!", sample.metric)};
    let metric_type = sample.labels.iter().find(|(label, _)| *label == "metric_type").map(|(_, value)| value).unwrap();
    statistics
        .entry((
            hostname.to_string(),
            sample.metric.clone(),
            metric_type.clone(),
            "".to_string(),
        ))
        .and_modify( |row| {
            row.delta_value = value - row.last_value;
            row.per_second_value = row.delta_value / (sample.timestamp.signed_duration_since(row.last_timestamp).num_milliseconds() as f64 / 1000.);
            row.last_value = value;
            row.last_timestamp = sample.timestamp;
            row.first_value = false;
            debug!("{}: {} last_value: {}, last_timestamp: {}, delta_value: {}, per_second_value: {}", hostname, sample.metric, row.last_value, row.last_timestamp, row.delta_value, row.per_second_value);
        } )
        .or_insert(
            Statistic
            {
                last_value: value,
                last_timestamp: sample.timestamp,
                first_value: true,
                ..Default::default()
            });
}

pub fn print_yb_network(
    statistics: &BTreeMap<(String, String, String, String), Statistic>,
)
{
    for hostname in statistics.iter().map(|((hostname, _, _, _), _)| hostname).unique()
    {
        if statistics.iter().filter(|((host, metric, metric_type, _), _)| host == hostname && metric == "tcp_bytes_received" && metric_type == "server").count() > 0
        {
            let network_bytes_received = statistics.iter().filter(|((host, metric, metric_type, _), _)| host == hostname && metric == "tcp_bytes_received" && metric_type == "server").map(|((_, _, _, _), statistic)| statistic.per_second_value).next().unwrap();
            let network_bytes_sent = statistics.iter().filter(|((host, metric, metric_type, _), _)| host == hostname && metric == "tcp_bytes_sent" && metric_type == "server").map(|((_, _, _, _), statistic)| statistic.per_second_value).next().unwrap();
            let network_bytes_total = network_bytes_received + network_bytes_sent;
            let time = statistics.iter().filter(|((host, metric, metric_type, _), _)| host == hostname && metric == "tcp_bytes_received" && metric_type == "server").map(|((_, _, _, _), statistic)| statistic.last_timestamp).next().unwrap();
            println!("{:50} {:8} {:10.2} {:10.2} {:10.2}",
                     hostname,
                     time.format("%H:%M:%S"),
                     network_bytes_received / (1024.*1024.),
                     network_bytes_sent / (1024.*1024.),
                     network_bytes_total / (1024.*1024.),
            );
        }
    }
}

pub fn print_yb_network_header()
{
    println!("{:50} {:8} {:>10} {:>10} {:>10}",
             "hostname",
             "time",
             "rxMB/s",
             "txMB/s",
             "totMB/s",
    );
}
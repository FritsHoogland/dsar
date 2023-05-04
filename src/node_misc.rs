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
    match sample.metric.as_str()
    {
        "node_intr_total" |
        "node_context_switches_total" => {
            let Value::Counter(value) = sample.value else { panic!("{} value enum type should be Counter!", sample.metric)};
            statistics
                .entry((
                    hostname.to_string(),
                    sample.metric.clone(),
                    "".to_string(),
                    "".to_string(),
                ))
                .and_modify( |row| {
                    row.delta_value = value - row.last_value;
                    row.per_second_value = row.delta_value / (sample.timestamp.signed_duration_since(row.last_timestamp).num_milliseconds() as f64 / 1000.);
                    row.last_value = value;
                    row.last_timestamp = sample.timestamp;
                    debug!("{}: {} last_value: {}, last_timestamp: {}, delta_value: {}, per_second_value: {}", hostname, sample.metric, row.last_value, row.last_timestamp, row.delta_value, row.per_second_value);
                } )
                .or_insert(
                    Statistic
                    {
                        last_value: value,
                        last_timestamp: sample.timestamp,
                        ..Default::default()
                    }
                );
        },
        "node_procs_running" |
        "node_procs_blocked" |
        "node_load1" |
        "node_load5" |
        "node_load15" => {
            let Value::Gauge(value) = sample.value else { panic!("{} value enum type should be Gauge!", sample.metric)};
            statistics
                .entry((
                    hostname.to_string(),
                    sample.metric.clone(),
                    "".to_string(),
                    "".to_string(),
                ))
                .and_modify( |row| {
                    row.last_value = value;
                    row.last_timestamp = sample.timestamp;
                    debug!("{} last_value: {}, last_timestamp: {}", sample.metric, row.last_value, row.last_timestamp);
                } )
                .or_insert(
                    Statistic
                    {
                        last_value: value,
                        last_timestamp: sample.timestamp,
                        ..Default::default()
                    }
                );
        },
        &_ => {},
    }
}

pub fn print_sar_q(
    statistics: &BTreeMap<(String, String, String, String), Statistic>,
)
{
    for hostname in statistics.iter().map(|((hostname, _, _, _), _)| hostname).unique()
    {
        if statistics.iter().filter(|((host, metric, _, _), _)| host == hostname && metric == "node_load1").count() > 0
        {
            let runqueue_size = statistics.iter().filter(|((host, metric, _, _), _)| host == hostname && metric == "runqueue_size_does_not_exist").map(|((_, _, _, _), statistic)| statistic.last_value).next().unwrap_or_default();
            let tasklist_size = statistics.iter().filter(|((host, metric, _, _), _)| host == hostname && metric == "tasklist_size_does_not_exist").map(|((_, _, _, _), statistic)| statistic.last_value).next().unwrap_or_default();
            let node_load_1 = statistics.iter().filter(|((host, metric, _, _), _)| host == hostname && metric == "node_load1").map(|((_, _, _, _), statistic)| statistic.last_value).next().unwrap();
            let node_load_5 = statistics.iter().filter(|((host, metric, _, _), _)| host == hostname && metric == "node_load5").map(|((_, _, _, _), statistic)| statistic.last_value).next().unwrap();
            let node_load_15 = statistics.iter().filter(|((host, metric, _, _), _)| host == hostname && metric == "node_load15").map(|((_, _, _, _), statistic)| statistic.last_value).next().unwrap();
            let node_procs_blocked = statistics.iter().filter(|((host, metric, _, _), _)| host == hostname && metric == "node_procs_blocked").map(|((_, _, _, _), statistic)| statistic.last_value).next().unwrap();
            let time = statistics.iter().filter(|((host, metric, _, _), _)| host == hostname && metric == "node_load1").map(|((_, _, _, _), statistic)| statistic.last_timestamp).next().unwrap();
            println!("{:30} {:8} {:10.0} {:10.0} {:10.2} {:10.2} {:10.2} {:10.0}",
                     hostname,
                     time.format("%H:%M:%S"),
                     runqueue_size,
                     tasklist_size,
                     node_load_1,
                     node_load_5,
                     node_load_15,
                     node_procs_blocked,
            );
        }
    }
}

pub fn print_sar_q_header()
{
    println!("{:30} {:8} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10}",
             "hostname",
             "time",
             "runq-sz",
             "plist-sz",
             "ldavg-1",
             "ldavg-5",
             "ldavg-15",
             "blocked",
    );
}


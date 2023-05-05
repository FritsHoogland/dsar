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
    // This is the list of Yugabyte statistics that are stored for CPU:
    // "cpu_stime" |
    // "cpu_utime" |
    // "voluntary_context_switches" |
    // "involuntary_context_switches"
    // voluntary and involuntary context switches are not printed currently.

    let Value::Untyped(value) = sample.value else { panic!("{} value enum type should be Untyped!", sample.metric)};
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
            debug!("{}: {} last_value: {}, last_timestamp: {}, delta_value: {}, per_second_value: {}", hostname, sample.metric, row.last_value, row.last_timestamp, row.delta_value, row.per_second_value);
        } )
        .or_insert(
            Statistic
            {
                last_value: value,
                last_timestamp: sample.timestamp,
                ..Default::default()
            });

}

pub fn print_yb_cpu(
    statistics: &BTreeMap<(String, String, String, String), Statistic>,
)
{
    for hostname in statistics.iter().map(|((hostname, _, _, _), _)| hostname).unique()
    {
        if statistics.iter().filter(|((host, metric, metric_type, _), _)| host == hostname && metric == "cpu_utime" && metric_type == "server").count() > 0
        {
            let cpu_user = statistics.iter().filter(|((host, metric, metric_type, _), _)| host == hostname && metric == "cpu_utime" && metric_type == "server").map(|((_, _, _, _), statistic)| statistic.per_second_value).next().unwrap();
            let cpu_system = statistics.iter().filter(|((host, metric, metric_type, _), _)| host == hostname && metric == "cpu_stime" && metric_type == "server").map(|((_, _, _, _), statistic)| statistic.per_second_value).next().unwrap();
            let cpu_total = cpu_user + cpu_system;
            let time = statistics.iter().filter(|((host, metric, metric_type, _), _)| host == hostname && metric == "cpu_utime" && metric_type == "server").map(|((_, _, _, _), statistic)| statistic.last_timestamp).next().unwrap();
            println!("{:50} {:8} {:10.3} {:10.3} {:10.3}",
                     hostname,
                     time.format("%H:%M:%S"),
                     cpu_user / 1000.,
                     cpu_system / 1000.,
                     cpu_total / 1000.,
            );
        }
    }
}

pub fn print_yb_cpu_header()
{
    println!("{:50} {:8} {:>10} {:>10} {:>10}",
             "hostname",
             "time",
             "user/s",
             "sys/s",
             "tot/s",
    );
}
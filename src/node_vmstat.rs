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
    let Value::Untyped(value) = sample.value else { panic!("{} value enum type should be Untyped!", sample.metric)};
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
            });

}

pub fn print_sar_w(
    statistics: &BTreeMap<(String, String, String, String), Statistic>,
)
{
    for hostname in statistics.iter().map(|((hostname, _, _, _), _)| hostname).unique()
    {
        if statistics.iter().filter(|((host, metric, _, _), _)| host == hostname && metric == "node_vmstat_pswpin").count() > 0
        {
            let pages_swap_in = statistics.iter().filter(|((host, metric, _, _), _)| host == hostname && metric == "node_vmstat_pswpin").map(|((_, _, _, _), statistic)| statistic.per_second_value).next().unwrap();
            let pages_swap_out = statistics.iter().filter(|((host, metric, _, _), _)| host == hostname && metric == "node_vmstat_pswpout").map(|((_, _, _, _), statistic)| statistic.per_second_value).next().unwrap();
            let time = statistics.iter().filter(|((host, metric, _, _), _)| host == hostname && metric == "node_vmstat_pswpin").map(|((_, _, _, _), statistic)| statistic.last_timestamp).next().unwrap();
            println!("{:30} {:8} {:10.0} {:10.0}",
                     hostname,
                     time.format("%H:%M:%S"),
                     pages_swap_in,
                     pages_swap_out,
            );
        }
    }
}

pub fn print_sar_w_header()
{
    println!("{:30} {:8} {:>10} {:>10}",
             "hostname",
             "time",
             "pswpin/s",
             "pswpout/s",
    );
}

pub fn print_sar_b(
    statistics: &BTreeMap<(String, String, String, String), Statistic>,
)
{
    for hostname in statistics.iter().map(|((hostname, _, _, _), _)| hostname).unique()
    {
        if statistics.iter().filter(|((host, metric, _, _), _)| host == hostname && metric == "node_vmstat_pgpgin").count() > 0
        {
            let pages_paged_in = statistics.iter().filter(|((host, metric, _, _), _)| host == hostname && metric == "node_vmstat_pgpgin").map(|((_, _, _, _), statistic)| statistic.per_second_value).next().unwrap();
            let pages_paged_out = statistics.iter().filter(|((host, metric, _, _), _)| host == hostname && metric == "node_vmstat_pgpgout").map(|((_, _, _, _), statistic)| statistic.per_second_value).next().unwrap();
            let faults = statistics.iter().filter(|((host, metric, _, _), _)| host == hostname && metric == "node_vmstat_pgfault").map(|((_, _, _, _), statistic)| statistic.per_second_value).next().unwrap();
            let major_faults = statistics.iter().filter(|((host, metric, _, _), _)| host == hostname && metric == "node_vmstat_pgfault").map(|((_, _, _, _), statistic)| statistic.per_second_value).next().unwrap();
            let time = statistics.iter().filter(|((host, metric, _, _), _)| host == hostname && metric == "node_vmstat_pgpgin").map(|((_, _, _, _), statistic)| statistic.last_timestamp).next().unwrap();
            println!("{:30} {:8} {:10.2} {:10.2} {:10.2} {:10.2}",
                     hostname,
                     time.format("%H:%M:%S"),
                     pages_paged_in,
                     pages_paged_out,
                     faults,
                     major_faults,
            );
        }
    }
}

pub fn print_sar_b_header()
{
    println!("{:30} {:8} {:>10} {:>10} {:>10} {:>10}",
             "hostname",
             "time",
             "pgpgin/s",
             "pgpgout/s",
             "fault/s",
             "majflt/s",
    );
}

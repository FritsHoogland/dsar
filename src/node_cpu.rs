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
        "node_schedstat_running_seconds_total" |
        "node_schedstat_waiting_seconds_total" => {
            let Value::Counter(value) = sample.value else { panic!("{} value enum type should be Counter!", sample.metric) };
            let cpu_number = sample.labels.iter().find(|(label, _)| *label == "cpu").map(|(_, value)| value).unwrap();
            statistics
                .entry((hostname.to_string(), sample.metric.clone(), cpu_number.to_string(), "".to_string()))
                .and_modify(|row| {
                    row.delta_value = value - row.last_value;
                    row.per_second_value = row.delta_value / (sample.timestamp.signed_duration_since(row.last_timestamp).num_milliseconds() as f64 / 1000.0);
                    row.last_value = value;
                    row.last_timestamp = sample.timestamp;
                    debug!("{} cpu: {}: last_value: {}, last_timestamp: {}, delta_value: {}, per_second_value: {}", sample.metric, cpu_number, row.last_value, row.last_timestamp, row.delta_value, row.per_second_value);
                })
                .or_insert(
                    Statistic
                    {
                        last_value: value,
                        last_timestamp: sample.timestamp,
                        ..Default::default()
                    }
                );
        },
        "node_cpu_seconds_total" |
        "node_cpu_guest_seconds_total" => {
            let Value::Counter(value) = sample.value else { panic!("{} value enum type should be Counter!", sample.metric) };
            let cpu_number = sample.labels.iter().find(|(label, _)| *label == "cpu").map(|(_, value)| value).unwrap();
            let mode = sample.labels.iter().find(|(label, _)| *label == "mode").map(|(_, value)| value).unwrap();
            statistics
                .entry((hostname.to_string(), sample.metric.clone(), cpu_number.to_string(), mode.to_string()))
                .and_modify(|row| {
                    row.delta_value = value - row.last_value;
                    row.per_second_value = row.delta_value / (sample.timestamp.signed_duration_since(row.last_timestamp).num_milliseconds() as f64 / 1000.0);
                    row.last_value = value;
                    row.last_timestamp = sample.timestamp;
                    debug!("{} mode: {}, cpu: {}: last_value: {}, last_timestamp: {}, delta_value: {}, per_second_value: {}", sample.metric, mode, cpu_number, row.last_value, row.last_timestamp, row.delta_value, row.per_second_value);
                })
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

pub fn create_total(
    sample: &Sample,
    host: &str,
    statistics: &mut BTreeMap<(String, String, String, String), Statistic>,
)
{
    match sample.metric.as_str()
    {
        "node_schedstat_running_seconds_total" |
        "node_schedstat_waiting_seconds_total" => {
            let last_timestamp = statistics.iter().find(|((hostname, metric, cpu, _), _)| hostname == host && metric == &sample.metric && cpu != "total").map(|((_, _, _, _), statistic)| statistic.last_timestamp).unwrap();
                let per_second_value = statistics.iter().filter(|((hostname, metric, cpu, _), _)| hostname == host && metric == &sample.metric && cpu != "total").map(|((_, _, _, _), statistic)| statistic.per_second_value).sum();
                statistics.entry((host.to_string(), sample.metric.to_string(), "total".to_string(), "".to_string()))
                    .and_modify(|row| { row.per_second_value = per_second_value; row.last_timestamp = last_timestamp; })
                    .or_insert(Statistic { per_second_value, last_timestamp, ..Default::default() });
        },
        "node_cpu_seconds_total" |
        "node_cpu_guest_seconds_total" => {
            let mode = sample.labels.iter().find(|(label, _)| *label == "mode").map(|(_, value)| value).unwrap();
            let last_timestamp = statistics.iter().find(|((hostname, metric, cpu, run_mode), _)| hostname == host && metric == &sample.metric && cpu != "total" && run_mode == mode).map(|((_, _, _, _), statistic)| statistic.last_timestamp).unwrap();
                let per_second_value = statistics.iter().filter(|((hostname, metric, cpu, run_mode), _)| hostname == host && metric == &sample.metric && cpu != "total" && run_mode == mode).map(|((_, _, _, _), statistic)| statistic.per_second_value).sum();
                statistics.entry((host.to_string(), sample.metric.to_string(), "total".to_string(), mode.to_string()))
                    .and_modify(|row| { row.per_second_value = per_second_value; row.last_timestamp = last_timestamp; })
                    .or_insert(Statistic { per_second_value, last_timestamp, ..Default::default() });
        },
        &_ => {},
    }
}

pub fn print_sar_u(
    mode: &str,
    statistics: &BTreeMap<(String, String, String, String), Statistic>,
)
{
    for hostname in statistics.iter().map(|((hostname, _, _, _), _)| hostname).unique()
    {
        if statistics.iter().filter(|((host, metric, _, _), _)| host == hostname && metric == "node_cpu_seconds_total").count() > 0
        {
            let user_time = statistics.iter().find(|((host, metric, cpu, mode), _)| host == hostname && metric == "node_cpu_seconds_total" && mode == "user" && cpu == "total").map(|((_, _, _, _), statistic)| statistic.per_second_value).unwrap();
            let system_time = statistics.iter().find(|((host, metric, cpu, mode), _)| host == hostname && metric == "node_cpu_seconds_total" && mode == "system" && cpu == "total").map(|((_, _, _, _), statistic)| statistic.per_second_value).unwrap();
            let iowait_time = statistics.iter().find(|((host, metric, cpu, mode), _)| host == hostname && metric == "node_cpu_seconds_total" && mode == "iowait" && cpu == "total").map(|((_, _, _, _), statistic)| statistic.per_second_value).unwrap();
            let irq_time = statistics.iter().find(|((host, metric, cpu, mode), _)| host == hostname && metric == "node_cpu_seconds_total" && mode == "irq" && cpu == "total").map(|((_, _, _, _), statistic)| statistic.per_second_value).unwrap();
            let nice_time = statistics.iter().find(|((host, metric, cpu, mode), _)| host == hostname && metric == "node_cpu_seconds_total" && mode == "nice" && cpu == "total").map(|((_, _, _, _), statistic)| statistic.per_second_value).unwrap();
            let softirq_time = statistics.iter().find(|((host, metric, cpu, mode), _)| host == hostname && metric == "node_cpu_seconds_total" && mode == "softirq" && cpu == "total").map(|((_, _, _, _), statistic)| statistic.per_second_value).unwrap();
            let steal_time = statistics.iter().find(|((host, metric, cpu, mode), _)| host == hostname && metric == "node_cpu_seconds_total" && mode == "steal" && cpu == "total").map(|((_, _, _, _), statistic)| statistic.per_second_value).unwrap();
            let idle_time = statistics.iter().find(|((host, metric, cpu, mode), _)| host == hostname && metric == "node_cpu_seconds_total" && mode == "idle" && cpu == "total").map(|((_, _, _, _), statistic)| statistic.per_second_value).unwrap();
            let guest_user = statistics.iter().find(|((host, metric, cpu, mode), _)| host == hostname && metric == "node_cpu_guest_seconds_total" && mode == "user" && cpu == "total").map(|((_, _, _, _), statistic)| statistic.per_second_value).unwrap();
            let guest_nice = statistics.iter().find(|((host, metric, cpu, mode), _)| host == hostname && metric == "node_cpu_guest_seconds_total" && mode == "nice" && cpu == "total").map(|((_, _, _, _), statistic)| statistic.per_second_value).unwrap();
            let schedstat_running = statistics.iter().find(|((host, metric, cpu, _), _)| host == hostname && metric == "node_schedstat_running_seconds_total" && cpu == "total").map(|((_, _, _, _), statistic)| statistic.per_second_value).unwrap();
            let schedstat_waiting = statistics.iter().find(|((host, metric, cpu, _), _)| host == hostname && metric == "node_schedstat_waiting_seconds_total" && cpu == "total").map(|((_, _, _, _), statistic)| statistic.per_second_value).unwrap();
            let total_time = user_time + system_time + iowait_time + irq_time + nice_time + softirq_time + steal_time + idle_time + guest_user + guest_nice;
            let time = statistics.iter().find(|((host, metric, cpu, mode), _)| host == hostname && metric == "node_cpu_seconds_total" && mode == "user" && cpu == "total").map(|((_, _, _, _), statistic)| statistic.last_timestamp).unwrap();
            match mode
            {
                "normal" => {
                    println!("{:30} {:8} {:3} {:9.2} {:9.2} {:9.2} {:9.2} {:9.2} {:9.2}",
                             hostname,
                             time.format("%H:%M:%S"),
                             "all",
                             user_time / total_time * 100.,
                             nice_time / total_time * 100.,
                             system_time / total_time * 100.,
                             iowait_time / total_time * 100.,
                             steal_time / total_time * 100.,
                             idle_time / total_time * 100.,
                    );
                },
                "all" => {
                    println!("{:30} {:8} {:3} {:9.2} {:9.2} {:9.2} {:9.2} {:9.2} {:9.2} {:9.2} {:9.2} {:9.2} {:9.2}",
                             hostname,
                             time.format("%H:%M:%S"),
                             "all",
                             user_time / total_time * 100.,
                             nice_time / total_time * 100.,
                             system_time / total_time * 100.,
                             iowait_time / total_time * 100.,
                             steal_time / total_time * 100.,
                             irq_time / total_time * 100.,
                             softirq_time / total_time * 100.,
                             guest_user / total_time * 100.,
                             guest_nice / total_time * 100.,
                             idle_time / total_time * 100.,
                    );
                },
                "extended" => {
                    println!("{:30} {:8} {:9.2} {:9.2} {:9.2} {:9.2} {:9.2} {:9.2} {:9.2} {:9.2} {:9.2} {:9.2} {:9.2} {:9.2}",
                             hostname,
                             time.format("%H:%M:%S"),
                             user_time,
                             nice_time,
                             system_time,
                             iowait_time,
                             steal_time,
                             irq_time,
                             softirq_time,
                             guest_user,
                             guest_nice,
                             idle_time,
                             schedstat_running,
                             schedstat_waiting,
                    );
                },
                &_ => {},
            }
        }
    }
}
pub fn print_sar_u_header(
   mode: &str,
)
{
    match mode
    {
        "normal" => {
            println!("{:30} {:8} {:3} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9}",
                     "hostname",
                     "time",
                     "CPU",
                     "%usr",
                     "%nice",
                     "%sys",
                     "%iowait",
                     "%steal",
                     "%idle",
            );
        }
        "all" => {
            println!("{:30} {:8} {:3} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9}",
                     "hostname",
                     "time",
                     "CPU",
                     "%usr",
                     "%nice",
                     "%sys",
                     "%iowait",
                     "%steal",
                     "%irq",
                     "%soft",
                     "%guest",
                     "%gnice",
                     "%idle",
            );
        },
        "extended" => {
            println!("{:30} {:8} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9}",
                     "hostname",
                     "time",
                     "usr",
                     "nice",
                     "sys",
                     "iowait",
                     "steal",
                     "irq",
                     "soft",
                     "guest",
                     "gnice",
                     "idle",
                     "sch_run",
                     "sch_wait",
            );
        },
        &_ => {},
    }
}

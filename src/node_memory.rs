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
    let Value::Gauge(value) = sample.value else { panic!("{} value enum type should be Gauge!", sample.metric) };
    statistics
        .entry((
            hostname.to_string(),
            sample.metric.clone(),
            "".to_string(),
            "".to_string(),
        ))
        .and_modify(|row| {
            row.last_value = value;
            row.last_timestamp = sample.timestamp;
            debug!("{} last_value: {}, last_timestamp: {}", sample.metric, row.last_value, row.last_timestamp);
        })
        .or_insert(
            Statistic
            {
                last_value: value,
                last_timestamp: sample.timestamp,
                ..Default::default()
            });
}

pub fn print_sar_r(
    mode: &str,
    statistics: &BTreeMap<(String, String, String, String), Statistic>,
)
{
    for hostname in statistics.iter().map(|((hostname, _, _, _), _)| hostname).unique()
    {
        if statistics.iter().filter(|((host, metric, _, _), _)| host == hostname && metric == "node_memory_MemFree_bytes").count() > 0
        {
            let memory_free = statistics.iter().filter(|((host, metric, _, _), _)| host == hostname && metric == "node_memory_MemFree_bytes").map(|((_, _, _, _), statistic)| statistic.last_value).next().unwrap();
            let memory_available = statistics.iter().filter(|((host, metric, _, _), _)| host == hostname && metric == "node_memory_MemAvailable_bytes").map(|((_, _, _, _), statistic)| statistic.last_value).next().unwrap();
            let memory_total = statistics.iter().filter(|((host, metric, _, _), _)| host == hostname && metric == "node_memory_MemTotal_bytes").map(|((_, _, _, _), statistic)| statistic.last_value).next().unwrap();
            let memory_used = memory_total - memory_free;
            let memory_buffers = statistics.iter().filter(|((host, metric, _, _), _)| host == hostname && metric == "node_memory_Buffers_bytes").map(|((_, _, _, _), statistic)| statistic.last_value).next().unwrap();
            let memory_cached = statistics.iter().filter(|((host, metric, _, _), _)| host == hostname && metric == "node_memory_Cached_bytes").map(|((_, _, _, _), statistic)| statistic.last_value).next().unwrap();
            let memory_commit = statistics.iter().filter(|((host, metric, _, _), _)| host == hostname && metric == "node_memory_Committed_AS_bytes").map(|((_, _, _, _), statistic)| statistic.last_value).next().unwrap();
            let memory_swap_total = statistics.iter().filter(|((host, metric, _, _), _)| host == hostname && metric == "node_memory_SwapTotal_bytes").map(|((_, _, _, _), statistic)| statistic.last_value).next().unwrap();
            let memory_active = statistics.iter().filter(|((host, metric, _, _), _)| host == hostname && metric == "node_memory_Active_bytes").map(|((_, _, _, _), statistic)| statistic.last_value).next().unwrap();
            let memory_inactive = statistics.iter().filter(|((host, metric, _, _), _)| host == hostname && metric == "node_memory_Inactive_bytes").map(|((_, _, _, _), statistic)| statistic.last_value).next().unwrap();
            let memory_dirty = statistics.iter().filter(|((host, metric, _, _), _)| host == hostname && metric == "node_memory_Dirty_bytes").map(|((_, _, _, _), statistic)| statistic.last_value).next().unwrap();
            let memory_anonymous = statistics.iter().filter(|((host, metric, _, _), _)| host == hostname && metric == "node_memory_AnonPages_bytes").map(|((_, _, _, _), statistic)| statistic.last_value).next().unwrap();
            let memory_slab = statistics.iter().filter(|((host, metric, _, _), _)| host == hostname && metric == "node_memory_Slab_bytes").map(|((_, _, _, _), statistic)| statistic.last_value).next().unwrap();
            let memory_kernel_stack = statistics.iter().filter(|((host, metric, _, _), _)| host == hostname && metric == "node_memory_KernelStack_bytes").map(|((_, _, _, _), statistic)| statistic.last_value).next().unwrap();
            let memory_pagetables = statistics.iter().filter(|((host, metric, _, _), _)| host == hostname && metric == "node_memory_PageTables_bytes").map(|((_, _, _, _), statistic)| statistic.last_value).next().unwrap();
            let memory_virtual_memory = statistics.iter().filter(|((host, metric, _, _), _)| host == hostname && metric == "node_memory_VmallocTotal_bytes").map(|((_, _, _, _), statistic)| statistic.last_value).next().unwrap();
            let time = statistics.iter().filter(|((host, metric, _, _), _)| host == hostname && metric == "node_memory_MemFree_bytes").map(|((_, _, _, _), statistic)| statistic.last_timestamp).next().unwrap();
            if mode == "normal"
            {
                println!("{:30} {:8} {:9.0} {:9.0} {:9.0} {:9.2} {:9.0} {:9.0} {:9.0} {:9.2} {:9.0} {:9.0} {:9.0}",
                         hostname,
                         time.format("%H:%M:%S"),
                         memory_free / (1024. * 1024.),
                         memory_available / (1024. * 1024.),
                         memory_used / (1024. * 1024.),
                         memory_used / memory_total * 100.,
                         memory_buffers / (1024. * 1024.),
                         memory_cached / (1024. * 1024.),
                         memory_commit / (1024. * 1024.),
                         memory_commit / (memory_total + memory_swap_total) * 100.,
                         memory_active / (1024. * 1024.),
                         memory_inactive / (1024. * 1024.),
                         memory_dirty / (1024. * 1024.),
                );
            } else {
                println!("{:30} {:8} {:9.0} {:9.0} {:9.0} {:9.2} {:9.0} {:9.0} {:9.0} {:9.2} {:9.0} {:9.0} {:9.0} {:9.0} {:9.0} {:9.0} {:9.0} {:9.0}",
                         hostname,
                         time.format("%H:%M:%S"),
                         memory_free / (1024. * 1024.),
                         memory_available / (1024. * 1024.),
                         memory_used / (1024. * 1024.),
                         memory_used / memory_total * 100.,
                         memory_buffers / (1024. * 1024.),
                         memory_cached / (1024. * 1024.),
                         memory_commit / (1024. * 1024.),
                         memory_commit / (memory_total + memory_swap_total) * 100.,
                         memory_active / (1024. * 1024.),
                         memory_inactive / (1024. * 1024.),
                         memory_dirty / (1024. * 1024.),
                         memory_anonymous / (1024. * 1024.),
                         memory_slab / (1024. * 1024.),
                         memory_kernel_stack / (1024. * 1024.),
                         memory_pagetables / (1024. * 1024.),
                         memory_virtual_memory / (1024. * 1024.),
                );
            }
        }
    }
}

pub fn print_sar_r_header(
    mode: &str,
)
{
    if mode == "normal"
    {
        println!("{:30} {:8} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9}",
                 "hostname",
                 "time",
                 "mbmemfree",
                 "mbavail",
                 "mbmemused",
                 "%memused",
                 "mbbuffers",
                 "mbcached",
                 "mbcommit",
                 "%commit",
                 "mbactive",
                 "mbinact",
                 "mbdirty",
        );
    } else {
        println!("{:30} {:8} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9}",
                 "hostname",
                 "time",
                 "mbmemfree",
                 "mbavail",
                 "mbmemused",
                 "%memused",
                 "mbbuffers",
                 "mbcached",
                 "mbcommit",
                 "%commit",
                 "mbactive",
                 "mbinact",
                 "mbdirty",
                 "mbanonpg",
                 "mbslab",
                 "mbstack",
                 "mbpgtbl",
                 "mbvmused",
        );
    }
}

pub fn print_sar_s(
    statistics: &BTreeMap<(String, String, String, String), Statistic>,
)
{
    for hostname in statistics.iter().map(|((hostname, _, _, _), _)| hostname).unique()
    {
        if statistics.iter().filter(|((host, metric, _, _), _)| host == hostname && metric == "node_memory_SwapFree_bytes").count() > 0
        {
            let swap_free = statistics.iter().filter(|((host, metric, _, _), _)| host == hostname && metric == "node_memory_SwapFree_bytes").map(|((_, _, _, _), statistic)| statistic.last_value).next().unwrap();
            let swap_total = statistics.iter().filter(|((host, metric, _, _), _)| host == hostname && metric == "node_memory_SwapTotal_bytes").map(|((_, _, _, _), statistic)| statistic.last_value).next().unwrap();
            let swap_used = swap_total - swap_free;
            let swap_used_percent = swap_used / swap_total * 100.;
            let swap_cached = statistics.iter().filter(|((host, metric, _, _), _)| host == hostname && metric == "node_memory_SwapCached_bytes").map(|((_, _, _, _), statistic)| statistic.last_value).next().unwrap();
            let swap_cached_percent = swap_cached / swap_used * 100.;
            let time = statistics.iter().filter(|((host, metric, _, _), _)| host == hostname && metric == "node_memory_SwapFree_bytes").map(|((_, _, _, _), statistic)| statistic.last_timestamp).next().unwrap();
            println!("{:30} {:8} {:10.0} {:10.0} {:10.2} {:10.0} {:10.2}",
                     hostname,
                     time.format("%H:%M:%S"),
                     swap_free / (1024. * 1024.),
                     swap_used / (1024. * 1024.),
                     swap_used_percent,
                     swap_cached / (1024. * 1024.),
                     if swap_cached_percent.is_nan() { 0. } else { swap_cached_percent },
            );
        }
    }
}

pub fn print_sar_s_header()
{
    println!("{:30} {:8} {:>10} {:>10} {:>10} {:>10} {:>10}",
             "hostname",
             "time",
             "mbspwfree",
             "mbswpused",
             "%swpused",
             "mbswpcad",
             "%swpcad",
    );
}
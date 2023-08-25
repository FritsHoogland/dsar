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

pub fn print_vmstat(
    statistics: &BTreeMap<(String, String, String, String), Statistic>,
)
{
    for hostname in statistics.iter().map(|((hostname, _, _, _), _)| hostname).unique()
    {
        if statistics.iter().any(|((host, metric, _, _), statistic)| host == hostname && metric == "node_procs_running" && !statistic.first_value)
        {
            let procs_running = statistics.iter().find(|((host, metric, _, _), _)| host == hostname && metric == "node_procs_running").map(|((_, _, _, _), statistic)| statistic.last_value).unwrap();
            let procs_blocked = statistics.iter().find(|((host, metric, _, _), _)| host == hostname && metric == "node_procs_blocked").map(|((_, _, _, _), statistic)| statistic.last_value).unwrap();
            let swap_free = statistics.iter().find(|((host, metric, _, _), _)| host == hostname && metric == "node_memory_SwapFree_bytes").map(|((_, _, _, _), statistic)| statistic.last_value).unwrap();
            let swap_total = statistics.iter().find(|((host, metric, _, _), _)| host == hostname && metric == "node_memory_SwapTotal_bytes").map(|((_, _, _, _), statistic)| statistic.last_value).unwrap();
            let memory_free = statistics.iter().find(|((host, metric, _, _), _)| host == hostname && metric == "node_memory_MemFree_bytes").map(|((_, _, _, _), statistic)| statistic.last_value).unwrap();
            let memory_buffers = statistics.iter().find(|((host, metric, _, _), _)| host == hostname && metric == "node_memory_Buffers_bytes").map(|((_, _, _, _), statistic)| statistic.last_value).unwrap();
            let memory_cache = statistics.iter().find(|((host, metric, _, _), _)| host == hostname && metric == "node_memory_Cached_bytes").map(|((_, _, _, _), statistic)| statistic.last_value).unwrap();
            //let memory_inactive = statistics.iter().find(|((host, metric, _, _), _)| host == hostname && metric == "node_memory_Inactive_bytes").map(|((_, _, _, _), statistic)| statistic.last_value).unwrap();
            //let memory_active = statistics.iter().find(|((host, metric, _, _), _)| host == hostname && metric == "node_memory_Active_bytes").map(|((_, _, _, _), statistic)| statistic.last_value).unwrap();
            let swap_in = statistics.iter().find(|((host, metric, _, _), _)| host == hostname && metric == "node_vmstat_pswpin").map(|((_, _, _, _), statistic)| statistic.per_second_value).unwrap();
            let swap_out = statistics.iter().find(|((host, metric, _, _), _)| host == hostname && metric == "node_vmstat_pswpout").map(|((_, _, _, _), statistic)| statistic.per_second_value).unwrap();
            let blockdevice_in = statistics.iter().find(|((host, metric, device, _), _)| host == hostname && metric == "node_disk_read_bytes_total" && device == "total").map(|((_, _, _, _), statistic)| statistic.per_second_value).unwrap();
            let blockdevice_out = statistics.iter().find(|((host, metric, device, _), _)| host == hostname && metric == "node_disk_written_bytes_total" && device == "total").map(|((_, _, _, _), statistic)| statistic.per_second_value).unwrap();
            let interrupts = statistics.iter().find(|((host, metric, _, _), _)| host == hostname && metric == "node_intr_total").map(|((_, _, _, _), statistic)| statistic.per_second_value).unwrap();
            let context_switches = statistics.iter().find(|((host, metric, _, _), _)| host == hostname && metric == "node_context_switches_total").map(|((_, _, _, _), statistic)| statistic.per_second_value).unwrap();
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
            let total_time = user_time + system_time + iowait_time + irq_time + nice_time + softirq_time + steal_time + idle_time + guest_user + guest_nice;
            let time = statistics.iter().find(|((host, metric, _, _), _)| host == hostname && metric == "node_procs_running").map(|((_, _, _, _), statistic)| statistic.last_timestamp).unwrap();
            println!("{:30} {:8} {:4.0} {:4.0} {:8.0} {:8.0} {:8.0} {:8.0} {:8.0} {:8.0} {:8.0} {:8.0} {:8.0} {:8.0} {:4.0} {:4.0} {:4.0} {:4.0} {:4.0} {:4.0}",
                     hostname,
                     time.format("%H:%M:%S"),
                     procs_running,
                     procs_blocked,
                     (swap_total-swap_free).max(0.) / (1024.*1024.),
                     memory_free / (1024.*1024.),
                     memory_buffers / (1024.*1024.),
                     memory_cache / (1024. * 1024.),
                     swap_in,
                     swap_out,
                     blockdevice_in / (1024. * 1024.),
                     blockdevice_out / (1024. * 1024.),
                     interrupts,
                     context_switches,
                     (user_time + nice_time) / total_time * 100.,
                     system_time / total_time * 100.,
                     idle_time / total_time * 100.,
                     iowait_time / total_time * 100.,
                     steal_time / total_time * 100.,
                     (guest_user + guest_nice) / total_time * 100.,
            );
        }
    }
}

pub fn print_vmstat_header()
{
    println!("{:30} {:8} {:9} {:35} {:17} {:17} {:17} {:25}",
             "",
             "",
             "--procs--",
             "------------memory (mb)------------",
             "-------swap------",
             "-----io (mb)-----",
             "------system-----",
             "--------------cpu------------",
    );
    println!("{:30} {:8} {:>4} {:>4} {:>8} {:>8} {:>8} {:>8} {:>8} {:>8} {:>8} {:>8} {:>8} {:>8} {:>4} {:>4} {:>4} {:>4} {:>4} {:>4}",
             "hostname",
             "time",
             "r",
             "b",
             "swpd",
             "free",
             "buff",
             "cache",
             "si",
             "so",
             "bi",
             "bo",
             "in",
             "cs",
             "us",
             "sy",
             "id",
             "wa",
             "st",
             "gu",
    );
}

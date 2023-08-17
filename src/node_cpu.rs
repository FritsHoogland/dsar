use std::{collections::BTreeMap, sync::{Arc, Mutex}};
use prometheus_parse::{Value, Sample};
use itertools::Itertools;
use log::*;
use plotters::prelude::*;
use plotters::chart::SeriesLabelPosition::UpperLeft;

use crate::{Statistic, HistoricalData, CAPTION_STYLE_FONT, CAPTION_STYLE_FONT_SIZE, MESH_STYLE_FONT_SIZE, LABELS_STYLE_FONT, LABELS_STYLE_FONT_SIZE, LABEL_AREA_SIZE_LEFT, LABEL_AREA_SIZE_BOTTOM, LABEL_AREA_SIZE_RIGHT, MESH_STYLE_FONT};

#[derive(Debug)]
pub struct NodeCpuDetails {
    pub user: f64,
    pub nice: f64,
    pub system: f64,
    pub iowait: f64,
    pub steal: f64,
    pub irq: f64,
    pub softirq: f64,
    pub guest_user: f64,
    pub guest_nice: f64,
    pub idle: f64,
    pub schedstat_runtime: f64,
    pub schedstat_wait: f64,
}

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
                    row.first_value = false;
                    debug!("{} cpu: {}: last_value: {}, last_timestamp: {}, delta_value: {}, per_second_value: {}", sample.metric, cpu_number, row.last_value, row.last_timestamp, row.delta_value, row.per_second_value);
                })
                .or_insert(
                    Statistic
                    {
                        last_value: value,
                        last_timestamp: sample.timestamp,
                        first_value: true,
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
                    row.first_value = false;
                    debug!("{} mode: {}, cpu: {}: last_value: {}, last_timestamp: {}, delta_value: {}, per_second_value: {}", sample.metric, mode, cpu_number, row.last_value, row.last_timestamp, row.delta_value, row.per_second_value);
                })
                .or_insert(
                    Statistic
                    {
                        last_value: value,
                        last_timestamp: sample.timestamp,
                        first_value: true,
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
                    .and_modify(|row| { row.per_second_value = per_second_value; row.last_timestamp = last_timestamp; row.first_value = false; })
                    .or_insert(Statistic { per_second_value, last_timestamp, first_value: true, ..Default::default() });
        },
        "node_cpu_seconds_total" |
        "node_cpu_guest_seconds_total" => {
            let mode = sample.labels.iter().find(|(label, _)| *label == "mode").map(|(_, value)| value).unwrap();
            let last_timestamp = statistics.iter().find(|((hostname, metric, cpu, run_mode), _)| hostname == host && metric == &sample.metric && cpu != "total" && run_mode == mode).map(|((_, _, _, _), statistic)| statistic.last_timestamp).unwrap();
            let per_second_value = statistics.iter().filter(|((hostname, metric, cpu, run_mode), _)| hostname == host && metric == &sample.metric && cpu != "total" && run_mode == mode).map(|((_, _, _, _), statistic)| statistic.per_second_value).sum();
            statistics.entry((host.to_string(), sample.metric.to_string(), "total".to_string(), mode.to_string()))
                    .and_modify(|row| { row.per_second_value = per_second_value; row.last_timestamp = last_timestamp; row.first_value = false; })
                    .or_insert(Statistic { per_second_value, last_timestamp, first_value: true, ..Default::default() });
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
        if statistics.iter().any(|((host, metric, _, _), row)| host == hostname && metric == "node_cpu_seconds_total" && !row.first_value)
        {
            let user_time = statistics.iter().find(|((host, metric, cpu, mode), _)| host == hostname && metric == "node_cpu_seconds_total" && mode == "user" && cpu == "total").map(|((_, _, _, _), statistic)| statistic.per_second_value).unwrap();
            let system_time = statistics.iter().find(|((host, metric, cpu, mode), _)| host == hostname && metric == "node_cpu_seconds_total" && mode == "system" && cpu == "total").map(|((_, _, _, _), statistic)| statistic.per_second_value).unwrap();
            let iowait_time = statistics.iter().find(|((host, metric, cpu, mode), _)| host == hostname && metric == "node_cpu_seconds_total" && mode == "iowait" && cpu == "total").map(|((_, _, _, _), statistic)| statistic.per_second_value).unwrap_or_default();
            let irq_time = statistics.iter().find(|((host, metric, cpu, mode), _)| host == hostname && metric == "node_cpu_seconds_total" && mode == "irq" && cpu == "total").map(|((_, _, _, _), statistic)| statistic.per_second_value).unwrap_or_default();
            let nice_time = statistics.iter().find(|((host, metric, cpu, mode), _)| host == hostname && metric == "node_cpu_seconds_total" && mode == "nice" && cpu == "total").map(|((_, _, _, _), statistic)| statistic.per_second_value).unwrap();
            let softirq_time = statistics.iter().find(|((host, metric, cpu, mode), _)| host == hostname && metric == "node_cpu_seconds_total" && mode == "softirq" && cpu == "total").map(|((_, _, _, _), statistic)| statistic.per_second_value).unwrap_or_default();
            let steal_time = statistics.iter().find(|((host, metric, cpu, mode), _)| host == hostname && metric == "node_cpu_seconds_total" && mode == "steal" && cpu == "total").map(|((_, _, _, _), statistic)| statistic.per_second_value).unwrap_or_default();
            let idle_time = statistics.iter().find(|((host, metric, cpu, mode), _)| host == hostname && metric == "node_cpu_seconds_total" && mode == "idle" && cpu == "total").map(|((_, _, _, _), statistic)| statistic.per_second_value).unwrap();
            let guest_user = statistics.iter().find(|((host, metric, cpu, mode), _)| host == hostname && metric == "node_cpu_guest_seconds_total" && mode == "user" && cpu == "total").map(|((_, _, _, _), statistic)| statistic.per_second_value).unwrap_or_default();
            let guest_nice = statistics.iter().find(|((host, metric, cpu, mode), _)| host == hostname && metric == "node_cpu_guest_seconds_total" && mode == "nice" && cpu == "total").map(|((_, _, _, _), statistic)| statistic.per_second_value).unwrap_or_default();
            let schedstat_running = statistics.iter().find(|((host, metric, cpu, _), _)| host == hostname && metric == "node_schedstat_running_seconds_total" && cpu == "total").map(|((_, _, _, _), statistic)| statistic.per_second_value).unwrap_or_default();
            let schedstat_waiting = statistics.iter().find(|((host, metric, cpu, _), _)| host == hostname && metric == "node_schedstat_waiting_seconds_total" && cpu == "total").map(|((_, _, _, _), statistic)| statistic.per_second_value).unwrap_or_default();
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

pub fn create_cpu_plots(
    historical_data: &Arc<Mutex<HistoricalData>>,
)
{
    let unlocked_historical_data = historical_data.lock().unwrap();
    for filter_hostname in unlocked_historical_data.cpu_details.keys().map(|(hostname, _)| hostname).unique()
    {
        // if no data was obtained, return immediately
        //if unlocked_historical_data.cpu_details.iter().count() == 0 { return };
        // set the plot specifics
        let start_time = unlocked_historical_data.cpu_details
            .keys()
            .filter(|(hostname,_)| hostname == filter_hostname)
            .map(|(_, timestamp)| timestamp)
            .min()
            .unwrap();
        let end_time = unlocked_historical_data.cpu_details
            .keys()
            .filter(|(hostname,_)| hostname == filter_hostname)
            .map(|(_, timestamp)| timestamp)
            .max()
            .unwrap();
        let low_value: f64 = 0.0;
        let high_value_cpu = unlocked_historical_data.cpu_details.iter()
            .filter(|((hostname, _),_)| hostname == filter_hostname)
            .map(|((_, _), row)| row.user + row.nice + row.system + row.iowait + row.steal + row.irq + row.softirq + row.guest_user + row.guest_nice + row.idle)
            .max_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        let high_value_schedstat = unlocked_historical_data.cpu_details.iter()
            .filter(|((hostname, _),_)| hostname == filter_hostname)
            .map(|((_, _), row)| row.schedstat_runtime + row.schedstat_wait)
            .max_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        let high_value = vec![high_value_cpu, high_value_schedstat].into_iter().max_by(|a, b| a.partial_cmp(b).unwrap()).unwrap();
        let filename = format!("{}_cpu.png", filter_hostname);

        // create the plot
        let root = BitMapBackend::new(&filename, (1280,900)).into_drawing_area();
        root.fill(&WHITE).unwrap();
        let mut contextarea = ChartBuilder::on(&root)
            .set_label_area_size(LabelAreaPosition::Left, LABEL_AREA_SIZE_LEFT)
            .set_label_area_size(LabelAreaPosition::Bottom, LABEL_AREA_SIZE_BOTTOM)
            .set_label_area_size(LabelAreaPosition::Right, LABEL_AREA_SIZE_RIGHT)
            .caption(format!("CPU usage: {}",filter_hostname), (CAPTION_STYLE_FONT, CAPTION_STYLE_FONT_SIZE))
            .build_cartesian_2d(*start_time..*end_time, low_value..high_value)
            .unwrap();
        contextarea.configure_mesh()
            .x_labels(4)
            .x_label_formatter(&|x| x.to_rfc3339())
            .y_desc("CPU per second")
            .label_style((MESH_STYLE_FONT, MESH_STYLE_FONT_SIZE))
            .draw()
            .unwrap();
        let min_scheduler_wait = unlocked_historical_data.cpu_details.iter()
            .filter(|((hostname, _), _)| hostname == filter_hostname)
            .map(|((_, _), row)| row.schedstat_wait)
            .min_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        let max_scheduler_wait = unlocked_historical_data.cpu_details.iter()
            .filter(|((hostname, _), _)| hostname == filter_hostname)
            .map(|((_, _), row)| row.schedstat_wait)
            .max_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        contextarea.draw_series(AreaSeries::new(unlocked_historical_data.cpu_details.iter()
                                                    .filter(|((hostname, _), _)| hostname == filter_hostname)
                                                    .map(|((_, timestamp), row)| (*timestamp, row.schedstat_wait + row.schedstat_runtime)),
                                                0.0, Palette99::pick(1))
        )
            .unwrap()
            .label(format!("{:25} min: {:10.2}, max: {:10.2}", "scheduler wait", min_scheduler_wait, max_scheduler_wait))
            .legend(move |(x, y)| Rectangle::new([(x - 3, y - 3), (x + 3, y + 3)], Palette99::pick(1).filled()));
        let min_scheduler_runtime = unlocked_historical_data.cpu_details.iter()
            .filter(|((hostname, _), _)| hostname == filter_hostname)
            .map(|((_, _), row)| row.schedstat_runtime)
            .min_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        let max_scheduler_runtime = unlocked_historical_data.cpu_details.iter()
            .filter(|((hostname, _), _)| hostname == filter_hostname)
            .map(|((_, _), row)| row.schedstat_runtime)
            .max_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        contextarea.draw_series(AreaSeries::new(unlocked_historical_data.cpu_details.iter()
                                                    .filter(|((hostname, _), _)| hostname == filter_hostname)
                                                    .map(|((_, timestamp), row)| (*timestamp, row.schedstat_runtime)),
                                                0.0, Palette99::pick(2))
        )
            .unwrap()
            .label(format!("{:25} min: {:10.2}, max: {:10.2}", "scheduler runtime", min_scheduler_runtime, max_scheduler_runtime))
            .legend(move |(x, y)| Rectangle::new([(x - 3, y - 3), (x + 3, y + 3)], Palette99::pick(2).filled()));
        let min_guest_nice = unlocked_historical_data.cpu_details.iter()
            .filter(|((hostname, _), _)| hostname == filter_hostname)
            .map(|((_, _), row)| row.guest_nice)
            .min_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        let max_guest_nice = unlocked_historical_data.cpu_details.iter()
            .filter(|((hostname, _), _)| hostname == filter_hostname)
            .map(|((_, _), row)| row.guest_nice)
            .max_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        contextarea.draw_series(AreaSeries::new(unlocked_historical_data.cpu_details.iter()
                                                    .filter(|((hostname, _), _)| hostname == filter_hostname)
                                                    .map(|((_, timestamp), row)| (*timestamp, row.guest_nice + row.guest_user + row.softirq + row.irq + row.steal + row.iowait + row.system + row.nice + row.user)),
                                                0.0, Palette99::pick(3))
        )
            .unwrap()
            .label(format!("{:25} min: {:10.2}, max: {:10.2}", "guest nice", min_guest_nice, max_guest_nice))
            .legend(move |(x, y)| Rectangle::new([(x - 3, y - 3), (x + 3, y + 3)], Palette99::pick(3).filled()));
        let min_guest_user = unlocked_historical_data.cpu_details.iter()
            .filter(|((hostname, _), _)| hostname == filter_hostname)
            .map(|((_, _), row)| row.guest_user)
            .min_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        let max_guest_user = unlocked_historical_data.cpu_details.iter()
            .filter(|((hostname, _), _)| hostname == filter_hostname)
            .map(|((_, _), row)| row.guest_user)
            .max_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        contextarea.draw_series(AreaSeries::new(unlocked_historical_data.cpu_details.iter()
                                                    .filter(|((hostname, _), _)| hostname == filter_hostname)
                                                    .map(|((_, timestamp), row)| (*timestamp, row.guest_user + row.softirq + row.irq + row.steal + row.iowait + row.system + row.nice + row.user)),
                                                0.0, Palette99::pick(4))
        )
            .unwrap()
            .label(format!("{:25} min: {:10.2}, max: {:10.2}", "guest user", min_guest_user, max_guest_user))
            .legend(move |(x, y)| Rectangle::new([(x - 3, y - 3), (x + 3, y + 3)], Palette99::pick(4).filled()));
        let min_softirq = unlocked_historical_data.cpu_details.iter()
            .filter(|((hostname, _), _)| hostname == filter_hostname)
            .map(|((_, _), row)| row.softirq)
            .min_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        let max_softirq = unlocked_historical_data.cpu_details.iter()
            .filter(|((hostname, _), _)| hostname == filter_hostname)
            .map(|((_, _), row)| row.softirq)
            .max_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        contextarea.draw_series(AreaSeries::new(unlocked_historical_data.cpu_details.iter()
                                                    .filter(|((hostname, _), _)| hostname == filter_hostname)
                                                    .map(|((_, timestamp), row)| (*timestamp, row.softirq + row.irq + row.steal + row.iowait + row.system + row.nice + row.user)),
                                                0.0, Palette99::pick(5))
        )
            .unwrap()
            .label(format!("{:25} min: {:10.2}, max: {:10.2}", "softirq", min_softirq, max_softirq))
            .legend(move |(x, y)| Rectangle::new([(x - 3, y - 3), (x + 3, y + 3)], Palette99::pick(5).filled()));
        let min_irq = unlocked_historical_data.cpu_details.iter()
            .filter(|((hostname, _), _)| hostname == filter_hostname)
            .map(|((_, _), row)| row.irq)
            .min_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        let max_irq = unlocked_historical_data.cpu_details.iter()
            .filter(|((hostname, _), _)| hostname == filter_hostname)
            .map(|((_, _), row)| row.irq)
            .max_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        contextarea.draw_series(AreaSeries::new(unlocked_historical_data.cpu_details.iter()
                                                    .filter(|((hostname, _), _)| hostname == filter_hostname)
                                                    .map(|((_, timestamp), row)| (*timestamp, row.irq + row.steal + row.iowait + row.system + row.nice + row.user)),
                                                0.0, Palette99::pick(6))
        )
            .unwrap()
            .label(format!("{:25} min: {:10.2}, max: {:10.2}", "irq", min_irq, max_irq))
            .legend(move |(x, y)| Rectangle::new([(x - 3, y - 3), (x + 3, y + 3)], Palette99::pick(6).filled()));
        let min_iowait = unlocked_historical_data.cpu_details.iter()
            .filter(|((hostname, _), _)| hostname == filter_hostname)
            .map(|((_, _), row)| row.iowait)
            .min_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        let max_iowait = unlocked_historical_data.cpu_details.iter()
            .filter(|((hostname, _), _)| hostname == filter_hostname)
            .map(|((_, _), row)| row.iowait)
            .max_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        contextarea.draw_series(AreaSeries::new(unlocked_historical_data.cpu_details.iter()
                                                    .filter(|((hostname, _), _)| hostname == filter_hostname)
                                                    .map(|((_, timestamp), row)| (*timestamp, row.iowait + row.system + row.nice + row.user)),
                                                0.0, Palette99::pick(8))
        )
            .unwrap()
            .label(format!("{:25} min: {:10.2}, max: {:10.2}", "iowait", min_iowait, max_iowait))
            .legend(move |(x, y)| Rectangle::new([(x - 3, y - 3), (x + 3, y + 3)], Palette99::pick(8).filled()));
        let min_system = unlocked_historical_data.cpu_details.iter()
            .filter(|((hostname, _), _)| hostname == filter_hostname)
            .map(|((_, _), row)| row.system)
            .min_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        let max_system = unlocked_historical_data.cpu_details.iter()
            .filter(|((hostname, _), _)| hostname == filter_hostname)
            .map(|((_, _), row)| row.system)
            .max_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        contextarea.draw_series(AreaSeries::new(unlocked_historical_data.cpu_details.iter()
                                                    .filter(|((hostname, _), _)| hostname == filter_hostname)
                                                    .map(|((_, timestamp), row)| (*timestamp, row.system + row.nice + row.user)),
                                                0.0, Palette99::pick(9))
        )
            .unwrap()
            .label(format!("{:25} min: {:10.2}, max: {:10.2}", "system", min_system, max_system))
            .legend(move |(x, y)| Rectangle::new([(x - 3, y - 3), (x + 3, y + 3)], Palette99::pick(9).filled()));
        let min_nice = unlocked_historical_data.cpu_details.iter()
            .filter(|((hostname, _), _)| hostname == filter_hostname)
            .map(|((_, _), row)| row.nice)
            .min_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        let max_nice = unlocked_historical_data.cpu_details.iter()
            .filter(|((hostname, _), _)| hostname == filter_hostname)
            .map(|((_, _), row)| row.nice)
            .max_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        contextarea.draw_series(AreaSeries::new(unlocked_historical_data.cpu_details.iter()
                                                    .filter(|((hostname, _), _)| hostname == filter_hostname)
                                                    .map(|((_, timestamp), row)| (*timestamp, row.nice + row.user)),
                                                0.0, Palette99::pick(10))
        )
            .unwrap()
            .label(format!("{:25} min: {:10.2}, max: {:10.2}", "nice", min_nice, max_nice))
            .legend(move |(x, y)| Rectangle::new([(x - 3, y - 3), (x + 3, y + 3)], Palette99::pick(10).filled()));
        let min_user = unlocked_historical_data.cpu_details.iter()
            .filter(|((hostname, _), _)| hostname == filter_hostname)
            .map(|((_, _), row)| row.user)
            .min_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        let max_user = unlocked_historical_data.cpu_details.iter()
            .filter(|((hostname, _), _)| hostname == filter_hostname)
            .map(|((_, _), row)| row.user)
            .max_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        contextarea.draw_series(AreaSeries::new(unlocked_historical_data.cpu_details.iter()
                                                    .filter(|((hostname, _), _)| hostname == filter_hostname)
                                                    .map(|((_, timestamp), row)| (*timestamp, row.user)),
                                                0.0, GREEN)
        )
            .unwrap()
            .label(format!("{:25} min: {:10.2}, max: {:10.2}", "user", min_user, max_user))
            .legend(move |(x, y)| Rectangle::new([(x - 3, y - 3), (x + 3, y + 3)], GREEN.filled()));
        contextarea.draw_series(AreaSeries::new(unlocked_historical_data.cpu_details.iter()
                                                    .filter(|((hostname, _), _)| hostname == filter_hostname)
                                                    .map(|((_, timestamp), row)| (*timestamp, (row.idle + row.guest_nice + row.guest_user + row.softirq + row.irq + row.steal + row.iowait + row.system + row.nice + row.user).round())),
                                                0.0, TRANSPARENT).border_style(RED)
        )
            .unwrap()
            .label("total cpu")
            .legend(move |(x, y)| Rectangle::new([(x - 3, y - 3), (x + 3, y + 3)], RED.filled()));
        contextarea.configure_series_labels()
            .border_style(BLACK)
            .background_style(WHITE.mix(0.7))
            .label_font((LABELS_STYLE_FONT, LABELS_STYLE_FONT_SIZE))
            .position(UpperLeft)
            .draw()
            .unwrap();

    }
}


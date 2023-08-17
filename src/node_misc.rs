use std::{collections::BTreeMap, sync::{Arc, Mutex}};
use prometheus_parse::{Value, Sample};
use itertools::Itertools;
use log::*;
use plotters::prelude::*;
use plotters::chart::SeriesLabelPosition::UpperLeft;

use crate::{Statistic, HistoricalData, CAPTION_STYLE_FONT, CAPTION_STYLE_FONT_SIZE, MESH_STYLE_FONT_SIZE, LABELS_STYLE_FONT, LABELS_STYLE_FONT_SIZE, LABEL_AREA_SIZE_LEFT, LABEL_AREA_SIZE_BOTTOM, LABEL_AREA_SIZE_RIGHT, MESH_STYLE_FONT};

#[derive(Debug)]
pub struct NodeMiscDetails {
    pub some_cpu: f64,
    pub some_io: f64,
    pub full_io: f64,
    pub some_mem: f64,
    pub full_mem: f64,
}

pub fn process_statistic(
    sample: &Sample,
    hostname: &str,
    statistics: &mut BTreeMap<(String, String, String, String), Statistic>,
)
{
    match sample.metric.as_str()
    {
        "node_pressure_cpu_waiting_seconds_total" |
        "node_pressure_io_stalled_seconds_total" |
        "node_pressure_io_waiting_seconds_total" |
        "node_pressure_memory_stalled_seconds_total" |
        "node_pressure_memory_waiting_seconds_total" |
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
                    row.first_value = false;
                    debug!("{} last_value: {}, last_timestamp: {}", sample.metric, row.last_value, row.last_timestamp);
                } )
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

pub fn print_sar_q(
    statistics: &BTreeMap<(String, String, String, String), Statistic>,
)
{
    for hostname in statistics.iter().map(|((hostname, _, _, _), _)| hostname).unique()
    {
        if statistics.iter().any(|((host, metric, _, _), _)| host == hostname && metric == "node_load1")
        {
            let runqueue_size = statistics.iter().find(|((host, metric, _, _), _)| host == hostname && metric == "runqueue_size_does_not_exist").map(|((_, _, _, _), statistic)| statistic.last_value).unwrap_or_default();
            let tasklist_size = statistics.iter().find(|((host, metric, _, _), _)| host == hostname && metric == "tasklist_size_does_not_exist").map(|((_, _, _, _), statistic)| statistic.last_value).unwrap_or_default();
            let node_load_1 = statistics.iter().find(|((host, metric, _, _), _)| host == hostname && metric == "node_load1").map(|((_, _, _, _), statistic)| statistic.last_value).unwrap();
            let node_load_5 = statistics.iter().find(|((host, metric, _, _), _)| host == hostname && metric == "node_load5").map(|((_, _, _, _), statistic)| statistic.last_value).unwrap();
            let node_load_15 = statistics.iter().find(|((host, metric, _, _), _)| host == hostname && metric == "node_load15").map(|((_, _, _, _), statistic)| statistic.last_value).unwrap();
            let node_procs_blocked = statistics.iter().find(|((host, metric, _, _), _)| host == hostname && metric == "node_procs_blocked").map(|((_, _, _, _), statistic)| statistic.last_value).unwrap_or_default();
            let time = statistics.iter().find(|((host, metric, _, _), _)| host == hostname && metric == "node_load1").map(|((_, _, _, _), statistic)| statistic.last_timestamp).unwrap();
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

pub fn print_psi(
    statistics: &BTreeMap<(String, String, String, String), Statistic>,
)
{
    for hostname in statistics.iter().map(|((hostname, _, _, _), _)| hostname).unique()
    {
        if statistics.iter().any(|((host, metric, _, _), _)| host == hostname && metric == "node_pressure_cpu_waiting_seconds_total")
        {
            let node_pressure_cpu_waiting_seconds_total = statistics.iter().find(|((host, metric, _, _), _)| host == hostname && metric == "node_pressure_cpu_waiting_seconds_total").map(|((_, _, _, _), statistic)| statistic.per_second_value).unwrap();
            let node_pressure_io_stalled_seconds_total = statistics.iter().find(|((host, metric, _, _), _)| host == hostname && metric == "node_pressure_io_stalled_seconds_total").map(|((_, _, _, _), statistic)| statistic.per_second_value).unwrap();
            let node_pressure_io_waiting_seconds_total = statistics.iter().find(|((host, metric, _, _), _)| host == hostname && metric == "node_pressure_io_waiting_seconds_total").map(|((_, _, _, _), statistic)| statistic.per_second_value).unwrap();
            let node_pressure_memory_stalled_seconds_total = statistics.iter().find(|((host, metric, _, _), _)| host == hostname && metric == "node_pressure_memory_stalled_seconds_total").map(|((_, _, _, _), statistic)| statistic.per_second_value).unwrap();
            let node_pressure_memory_waiting_seconds_total = statistics.iter().find(|((host, metric, _, _), _)| host == hostname && metric == "node_pressure_memory_waiting_seconds_total").map(|((_, _, _, _), statistic)| statistic.per_second_value).unwrap();

            let time = statistics.iter().find(|((host, metric, _, _), _)| host == hostname && metric == "node_pressure_cpu_waiting_seconds_total").map(|((_, _, _, _), statistic)| statistic.last_timestamp).unwrap();
            println!("{:30} {:8} {:10.3} {:10.3} {:10.3} {:10.3} {:10.3}",
                     hostname,
                     time.format("%H:%M:%S"),
                     node_pressure_cpu_waiting_seconds_total,
                     node_pressure_io_waiting_seconds_total,
                     node_pressure_io_stalled_seconds_total,
                     node_pressure_memory_waiting_seconds_total,
                     node_pressure_memory_stalled_seconds_total,
            );
        }
    }
}

pub fn print_psi_header()
{
    println!("{:30} {:8} {:>10} {:>10} {:>10} {:>10} {:>10}",
             "hostname",
             "time",
             "some cpu",
             "some io",
             "full io",
             "some mem",
             "full mem",
    );
}

pub fn create_misc_plots(
    historical_data: &Arc<Mutex<HistoricalData>>,
)
{
    let unlocked_historical_data = historical_data.lock().unwrap();
    for filter_hostname in unlocked_historical_data.misc_details.keys().map(|(hostname, _)| hostname).unique()
    {
        let start_time = unlocked_historical_data.misc_details
            .keys()
            .filter(|(hostname, _)| hostname == filter_hostname)
            .map(|(_, timestamp)| timestamp)
            .min()
            .unwrap();
        let end_time = unlocked_historical_data.misc_details
            .keys()
            .filter(|(hostname, _)| hostname == filter_hostname)
            .map(|(_, timestamp)| timestamp)
            .max()
            .unwrap();
        let low_value: f64 = 0.0;
        let high_value_some_cpu = unlocked_historical_data.misc_details.iter()
            .filter(|((hostname, _), _)| hostname == filter_hostname)
            .map(|((_, _), row)| row.some_cpu)
            .max_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        let high_value_some_io = unlocked_historical_data.misc_details.iter()
            .filter(|((hostname, _), _)| hostname == filter_hostname)
            .map(|((_, _), row)| row.some_io)
            .max_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        let high_value_full_io = unlocked_historical_data.misc_details.iter()
            .filter(|((hostname, _), _)| hostname == filter_hostname)
            .map(|((_, _), row)| row.full_io)
            .max_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        let high_value_some_mem = unlocked_historical_data.misc_details.iter()
            .filter(|((hostname, _), _)| hostname == filter_hostname)
            .map(|((_, _), row)| row.some_mem)
            .max_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        let high_value_full_mem = unlocked_historical_data.misc_details.iter()
            .filter(|((hostname, _), _)| hostname == filter_hostname)
            .map(|((_, _), row)| row.full_mem)
            .max_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        let high_value = vec![high_value_some_cpu, high_value_some_io, high_value_full_io, high_value_some_mem, high_value_full_mem].into_iter().max_by(|a, b| a.partial_cmp(b).unwrap()).unwrap();
        let filename = format!("{filter_hostname}_psi.png");

        let root = BitMapBackend::new(&filename, (1280,900)).into_drawing_area();
        root.fill(&WHITE).unwrap();
        let mut contextarea = ChartBuilder::on(&root)
            .set_label_area_size(LabelAreaPosition::Left, LABEL_AREA_SIZE_LEFT)
            .set_label_area_size(LabelAreaPosition::Bottom, LABEL_AREA_SIZE_BOTTOM)
            .set_label_area_size(LabelAreaPosition::Right, LABEL_AREA_SIZE_RIGHT)
            .caption(format!("Pressure stall information: {}",filter_hostname), (CAPTION_STYLE_FONT, CAPTION_STYLE_FONT_SIZE))
            .build_cartesian_2d(*start_time..*end_time, low_value..high_value)
            .unwrap();
        contextarea.configure_mesh()
            .x_labels(4)
            .x_label_formatter(&|x| x.to_rfc3339())
            .y_desc("Pressure stall per second")
            .label_style((MESH_STYLE_FONT, MESH_STYLE_FONT_SIZE))
            .draw()
            .unwrap();

        let min_some_cpu = unlocked_historical_data.misc_details.iter()
            .filter(|((hostname, _), _)| hostname == filter_hostname )
            .map(|((_, _), row)| row.some_cpu )
            .min_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        let max_some_cpu = unlocked_historical_data.misc_details.iter()
            .filter(|((hostname, _), _)| hostname == filter_hostname )
            .map(|((_, _), row)| row.some_cpu )
            .max_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        contextarea.draw_series(LineSeries::new(unlocked_historical_data.misc_details.iter()
                                                    .filter(|((hostname, _), _)| hostname == filter_hostname)
                                                    .map(|((_, timestamp), row)| (*timestamp, row.some_cpu )),
                                                Palette99::pick(1))
        )
            .unwrap()
            .label(format!("{:25} min: {:10.3}, max: {:10.3}", "Some CPU (s)", min_some_cpu, max_some_cpu))
            .legend(move |(x, y)| Rectangle::new([(x - 3, y - 3), (x + 3, y + 3)], Palette99::pick(1).filled()));
        let min_some_io = unlocked_historical_data.misc_details.iter()
            .filter(|((hostname, _), _)| hostname == filter_hostname )
            .map(|((_, _), row)| row.some_io )
            .min_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        let max_some_io = unlocked_historical_data.misc_details.iter()
            .filter(|((hostname, _), _)| hostname == filter_hostname )
            .map(|((_, _), row)| row.some_io )
            .max_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        contextarea.draw_series(LineSeries::new(unlocked_historical_data.misc_details.iter()
                                                    .filter(|((hostname, _), _)| hostname == filter_hostname)
                                                    .map(|((_, timestamp), row)| (*timestamp, row.some_io )),
                                                Palette99::pick(2))
        )
            .unwrap()
            .label(format!("{:25} min: {:10.3}, max: {:10.3}", "Some IO (s)", min_some_io, max_some_io))
            .legend(move |(x, y)| Rectangle::new([(x - 3, y - 3), (x + 3, y + 3)], Palette99::pick(2).filled()));
        let min_full_io = unlocked_historical_data.misc_details.iter()
            .filter(|((hostname, _), _)| hostname == filter_hostname )
            .map(|((_, _), row)| row.full_io )
            .min_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        let max_full_io = unlocked_historical_data.misc_details.iter()
            .filter(|((hostname, _), _)| hostname == filter_hostname )
            .map(|((_, _), row)| row.full_io )
            .max_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        contextarea.draw_series(LineSeries::new(unlocked_historical_data.misc_details.iter()
                                                    .filter(|((hostname, _), _)| hostname == filter_hostname)
                                                    .map(|((_, timestamp), row)| (*timestamp, row.full_io )),
                                                Palette99::pick(3))
        )
            .unwrap()
            .label(format!("{:25} min: {:10.3}, max: {:10.3}", "Full IO (s)", min_full_io, max_full_io))
            .legend(move |(x, y)| Rectangle::new([(x - 3, y - 3), (x + 3, y + 3)], Palette99::pick(3).filled()));
        let min_some_mem = unlocked_historical_data.misc_details.iter()
            .filter(|((hostname, _), _)| hostname == filter_hostname )
            .map(|((_, _), row)| row.some_mem )
            .min_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        let max_some_mem = unlocked_historical_data.misc_details.iter()
            .filter(|((hostname, _), _)| hostname == filter_hostname )
            .map(|((_, _), row)| row.some_mem )
            .max_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        contextarea.draw_series(LineSeries::new(unlocked_historical_data.misc_details.iter()
                                                    .filter(|((hostname, _), _)| hostname == filter_hostname)
                                                    .map(|((_, timestamp), row)| (*timestamp, row.some_mem )),
                                                Palette99::pick(4))
        )
            .unwrap()
            .label(format!("{:25} min: {:10.3}, max: {:10.3}", "Some memory (s)", min_some_mem, max_some_mem))
            .legend(move |(x, y)| Rectangle::new([(x - 3, y - 3), (x + 3, y + 3)], Palette99::pick(4).filled()));
        let min_full_mem = unlocked_historical_data.misc_details.iter()
            .filter(|((hostname, _), _)| hostname == filter_hostname )
            .map(|((_, _), row)| row.full_mem )
            .min_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        let max_full_mem = unlocked_historical_data.misc_details.iter()
            .filter(|((hostname, _), _)| hostname == filter_hostname )
            .map(|((_, _), row)| row.full_mem )
            .max_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        contextarea.draw_series(LineSeries::new(unlocked_historical_data.misc_details.iter()
                                                    .filter(|((hostname, _), _)| hostname == filter_hostname)
                                                    .map(|((_, timestamp), row)| (*timestamp, row.full_mem )),
                                                Palette99::pick(5))
        )
            .unwrap()
            .label(format!("{:25} min: {:10.3}, max: {:10.3}", "Full memory (s)", min_full_mem, max_full_mem))
            .legend(move |(x, y)| Rectangle::new([(x - 3, y - 3), (x + 3, y + 3)], Palette99::pick(5).filled()));

        contextarea.configure_series_labels()
            .border_style(BLACK)
            .background_style(WHITE.mix(0.7))
            .label_font((LABELS_STYLE_FONT, LABELS_STYLE_FONT_SIZE))
            .position(UpperLeft)
            .draw()
            .unwrap();
    }
}
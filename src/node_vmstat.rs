use std::{collections::BTreeMap, sync::{Arc, Mutex}};
use prometheus_parse::{Value, Sample};
use itertools::Itertools;
use log::*;
use plotters::prelude::*;
use plotters::chart::SeriesLabelPosition::UpperLeft;

use crate::{Statistic, HistoricalData, CAPTION_STYLE_FONT, CAPTION_STYLE_FONT_SIZE, MESH_STYLE_FONT_SIZE, LABELS_STYLE_FONT, LABELS_STYLE_FONT_SIZE, LABEL_AREA_SIZE_LEFT, LABEL_AREA_SIZE_BOTTOM, LABEL_AREA_SIZE_RIGHT, MESH_STYLE_FONT};

#[derive(Debug)]
pub struct NodeVmstatDetails {
    pub pages_swap_in: f64,
    pub pages_swap_out: f64,
    pub pages_page_in: f64,
    pub pages_page_out: f64,
    pub pages_minor_pagefault: f64,
    pub pages_major_pagefault: f64,
    pub oom_kill: f64,
}
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
        if statistics.iter().filter(|((host, metric, _, _), statistic)| host == hostname && metric == "node_vmstat_pgpgin" && !statistic.first_value).count() > 0
        {
            let time = statistics.iter().filter(|((host, metric, _, _), _)| host == hostname && metric == "node_vmstat_pgpgin").map(|((_, _, _, _), statistic)| statistic.last_timestamp).next().unwrap();

            let pages_paged_in = statistics.iter().filter(|((host, metric, _, _), _)| host == hostname && metric == "node_vmstat_pgpgin").map(|((_, _, _, _), statistic)| statistic.per_second_value).next().unwrap();
            let pages_paged_out = statistics.iter().filter(|((host, metric, _, _), _)| host == hostname && metric == "node_vmstat_pgpgout").map(|((_, _, _, _), statistic)| statistic.per_second_value).next().unwrap();
            let faults = statistics.iter().filter(|((host, metric, _, _), _)| host == hostname && metric == "node_vmstat_pgfault").map(|((_, _, _, _), statistic)| statistic.per_second_value).next().unwrap();
            let major_faults = statistics.iter().filter(|((host, metric, _, _), _)| host == hostname && metric == "node_vmstat_pgmajfault").map(|((_, _, _, _), statistic)| statistic.per_second_value).next().unwrap();
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


pub fn create_vmstat_plots(
    historical_data: &Arc<Mutex<HistoricalData>>,
)
{
    let unlocked_historical_data = historical_data.lock().unwrap();
    for filter_hostname in unlocked_historical_data.vmstat_details.keys().map(|(hostname, _)| hostname).unique()
    {
        // the start time and end time can be shared between the different plots.
        let start_time = unlocked_historical_data.vmstat_details
            .keys()
            .filter(|(hostname, _)| hostname == filter_hostname)
            .map(|(_, timestamp)| timestamp)
            .min()
            .unwrap();
        let end_time = unlocked_historical_data.vmstat_details
            .keys()
            .filter(|(hostname, _)| hostname == filter_hostname)
            .map(|(_, timestamp)| timestamp)
            .max()
            .unwrap();

        // the low value should be zero for the plots, which also is constant for the graphs
        let low_value: f64 = 0.0;

        // pages swapped in/out
        let high_value_pages_swap = unlocked_historical_data.vmstat_details.iter()
            .filter(|((hostname, _), _)| hostname == filter_hostname)
            .map(|((_, _), row)| ( row.pages_swap_in + row.pages_swap_out ) )
            .max_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        let filename = format!("{filter_hostname}_vmstat_pgswp.png");

        let root = BitMapBackend::new(&filename, (1280,900)).into_drawing_area();
        root.fill(&WHITE).unwrap();
        let mut contextarea = ChartBuilder::on(&root)
            .set_label_area_size(LabelAreaPosition::Left, LABEL_AREA_SIZE_LEFT)
            .set_label_area_size(LabelAreaPosition::Bottom, LABEL_AREA_SIZE_BOTTOM)
            .set_label_area_size(LabelAreaPosition::Right, LABEL_AREA_SIZE_RIGHT)
            .caption(format!("Pages swapped per second: {}",filter_hostname), (CAPTION_STYLE_FONT, CAPTION_STYLE_FONT_SIZE))
            .build_cartesian_2d(*start_time..*end_time, low_value..high_value_pages_swap)
            .unwrap();
        contextarea.configure_mesh()
            .x_labels(4)
            .x_label_formatter(&|x| x.to_rfc3339())
            .y_desc("pages per second")
            .label_style((MESH_STYLE_FONT, MESH_STYLE_FONT_SIZE))
            .draw()
            .unwrap();

        let min_pgswpin = unlocked_historical_data.vmstat_details.iter()
            .filter(|((hostname, _), _)| hostname == filter_hostname )
            .map(|((_, _), row)| row.pages_swap_in )
            .min_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        let max_pgswpin = unlocked_historical_data.vmstat_details.iter()
            .filter(|((hostname, _), _)| hostname == filter_hostname )
            .map(|((_, _), row)| row.pages_swap_in )
            .max_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        let min_pgswpout = unlocked_historical_data.vmstat_details.iter()
            .filter(|((hostname, _), _)| hostname == filter_hostname )
            .map(|((_, _), row)| row.pages_swap_out )
            .min_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        let max_pgswpout = unlocked_historical_data.vmstat_details.iter()
            .filter(|((hostname, _), _)| hostname == filter_hostname )
            .map(|((_, _), row)| row.pages_swap_out )
            .max_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        // pgswapout = pgswapin + pgswapout
        contextarea.draw_series(AreaSeries::new(unlocked_historical_data.vmstat_details.iter()
                                                    .filter(|((hostname, _), _)| hostname == filter_hostname)
                                                    .map(|((_, timestamp), row)| (*timestamp, (row.pages_swap_in + row.pages_swap_out ) )), 0.0, Palette99::pick(1))
        )
            .unwrap()
            .label(format!("{:25} min: {:10.3}, max: {:10.3}", "Pages swapped out/s", min_pgswpout, max_pgswpout))
            .legend(move |(x, y)| Rectangle::new([(x - 3, y - 3), (x + 3, y + 3)], Palette99::pick(1).filled()));
        // pgswapin
        contextarea.draw_series(AreaSeries::new(unlocked_historical_data.vmstat_details.iter()
                                                    .filter(|((hostname, _), _)| hostname == filter_hostname)
                                                    .map(|((_, timestamp), row)| (*timestamp, row.pages_swap_in )), 0.0, Palette99::pick(2))
        )
            .unwrap()
            .label(format!("{:25} min: {:10.3}, max: {:10.3}", "Pages swapped in/s", min_pgswpin, max_pgswpin))
            .legend(move |(x, y)| Rectangle::new([(x - 3, y - 3), (x + 3, y + 3)], Palette99::pick(2).filled()));
        contextarea.configure_series_labels()
            .border_style(BLACK)
            .background_style(WHITE.mix(0.7))
            .label_font((LABELS_STYLE_FONT, LABELS_STYLE_FONT_SIZE))
            .position(UpperLeft)
            .draw()
            .unwrap();

        // pages paged in/out
        let high_value_pages_paged = unlocked_historical_data.vmstat_details.iter()
            .filter(|((hostname, _), _)| hostname == filter_hostname)
            .map(|((_, _), row)| ( row.pages_page_in + row.pages_page_out ) )
            .max_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        let filename = format!("{filter_hostname}_vmstat_paging.png");

        let root = BitMapBackend::new(&filename, (1280,900)).into_drawing_area();
        root.fill(&WHITE).unwrap();
        let mut contextarea = ChartBuilder::on(&root)
            .set_label_area_size(LabelAreaPosition::Left, LABEL_AREA_SIZE_LEFT)
            .set_label_area_size(LabelAreaPosition::Bottom, LABEL_AREA_SIZE_BOTTOM)
            .set_label_area_size(LabelAreaPosition::Right, LABEL_AREA_SIZE_RIGHT)
            .caption(format!("Paging per second: {}",filter_hostname), (CAPTION_STYLE_FONT, CAPTION_STYLE_FONT_SIZE))
            .build_cartesian_2d(*start_time..*end_time, low_value..high_value_pages_paged)
            .unwrap();
        contextarea.configure_mesh()
            .x_labels(4)
            .x_label_formatter(&|x| x.to_rfc3339())
            .y_desc("paging per second")
            .label_style((MESH_STYLE_FONT, MESH_STYLE_FONT_SIZE))
            .draw()
            .unwrap();

        let min_pgpgin = unlocked_historical_data.vmstat_details.iter()
            .filter(|((hostname, _), _)| hostname == filter_hostname )
            .map(|((_, _), row)| row.pages_page_in )
            .min_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        let max_pgpgin = unlocked_historical_data.vmstat_details.iter()
            .filter(|((hostname, _), _)| hostname == filter_hostname )
            .map(|((_, _), row)| row.pages_page_in )
            .max_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        let min_pgpgout = unlocked_historical_data.vmstat_details.iter()
            .filter(|((hostname, _), _)| hostname == filter_hostname )
            .map(|((_, _), row)| row.pages_page_out )
            .min_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        let max_pgpgout = unlocked_historical_data.vmstat_details.iter()
            .filter(|((hostname, _), _)| hostname == filter_hostname )
            .map(|((_, _), row)| row.pages_page_out )
            .max_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        // pgpgout = pgspgin + pgpgout
        contextarea.draw_series(AreaSeries::new(unlocked_historical_data.vmstat_details.iter()
                                                    .filter(|((hostname, _), _)| hostname == filter_hostname)
                                                    .map(|((_, timestamp), row)| (*timestamp, (row.pages_page_in + row.pages_page_out ) )), 0.0, Palette99::pick(1))
        )
            .unwrap()
            .label(format!("{:25} min: {:10.3}, max: {:10.3}", "Pages paged out/s", min_pgpgout, max_pgpgout))
            .legend(move |(x, y)| Rectangle::new([(x - 3, y - 3), (x + 3, y + 3)], Palette99::pick(1).filled()));
        // pgswapin
        contextarea.draw_series(AreaSeries::new(unlocked_historical_data.vmstat_details.iter()
                                                    .filter(|((hostname, _), _)| hostname == filter_hostname)
                                                    .map(|((_, timestamp), row)| (*timestamp, row.pages_page_in )), 0.0, Palette99::pick(2))
        )
            .unwrap()
            .label(format!("{:25} min: {:10.3}, max: {:10.3}", "Pages paged in/s", min_pgpgin, max_pgpgin))
            .legend(move |(x, y)| Rectangle::new([(x - 3, y - 3), (x + 3, y + 3)], Palette99::pick(2).filled()));
        contextarea.configure_series_labels()
            .border_style(BLACK)
            .background_style(WHITE.mix(0.7))
            .label_font((LABELS_STYLE_FONT, LABELS_STYLE_FONT_SIZE))
            .position(UpperLeft)
            .draw()
            .unwrap();

        //  pagefaults
        let high_value_pagefaults = unlocked_historical_data.vmstat_details.iter()
            .filter(|((hostname, _), _)| hostname == filter_hostname)
            .map(|((_, _), row)| ( row.pages_minor_pagefault + row.pages_major_pagefault ) )
            .max_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        let filename = format!("{filter_hostname}_vmstat_pagefaults.png");

        let root = BitMapBackend::new(&filename, (1280,900)).into_drawing_area();
        root.fill(&WHITE).unwrap();
        let mut contextarea = ChartBuilder::on(&root)
            .set_label_area_size(LabelAreaPosition::Left, LABEL_AREA_SIZE_LEFT)
            .set_label_area_size(LabelAreaPosition::Bottom, LABEL_AREA_SIZE_BOTTOM)
            .set_label_area_size(LabelAreaPosition::Right, LABEL_AREA_SIZE_RIGHT)
            .caption(format!("Pagefaults per second: {}",filter_hostname), (CAPTION_STYLE_FONT, CAPTION_STYLE_FONT_SIZE))
            .build_cartesian_2d(*start_time..*end_time, low_value..high_value_pagefaults)
            .unwrap();
        contextarea.configure_mesh()
            .x_labels(4)
            .x_label_formatter(&|x| x.to_rfc3339())
            .y_desc("pagefaults per second")
            .label_style((MESH_STYLE_FONT, MESH_STYLE_FONT_SIZE))
            .draw()
            .unwrap();

        let min_minor_fault = unlocked_historical_data.vmstat_details.iter()
            .filter(|((hostname, _), _)| hostname == filter_hostname )
            .map(|((_, _), row)| row.pages_minor_pagefault )
            .min_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        let max_minor_fault = unlocked_historical_data.vmstat_details.iter()
            .filter(|((hostname, _), _)| hostname == filter_hostname )
            .map(|((_, _), row)| row.pages_minor_pagefault )
            .max_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        let min_major_fault = unlocked_historical_data.vmstat_details.iter()
            .filter(|((hostname, _), _)| hostname == filter_hostname )
            .map(|((_, _), row)| row.pages_major_pagefault )
            .min_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        let max_major_fault = unlocked_historical_data.vmstat_details.iter()
            .filter(|((hostname, _), _)| hostname == filter_hostname )
            .map(|((_, _), row)| row.pages_major_pagefault )
            .max_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        // minor faults = minor faults + major faults
        contextarea.draw_series(AreaSeries::new(unlocked_historical_data.vmstat_details.iter()
                                                    .filter(|((hostname, _), _)| hostname == filter_hostname)
                                                    .map(|((_, timestamp), row)| (*timestamp, (row.pages_minor_pagefault + row.pages_major_pagefault ) )), 0.0, Palette99::pick(1))
        )
            .unwrap()
            .label(format!("{:25} min: {:10.3}, max: {:10.3}", "minor pagefaults/s", min_minor_fault, max_minor_fault))
            .legend(move |(x, y)| Rectangle::new([(x - 3, y - 3), (x + 3, y + 3)], Palette99::pick(1).filled()));
        // major faults
        contextarea.draw_series(AreaSeries::new(unlocked_historical_data.vmstat_details.iter()
                                                    .filter(|((hostname, _), _)| hostname == filter_hostname)
                                                    .map(|((_, timestamp), row)| (*timestamp, row.pages_major_pagefault )), 0.0, Palette99::pick(2))
        )
            .unwrap()
            .label(format!("{:25} min: {:10.3}, max: {:10.3}", "major pagefaults/s", min_major_fault, max_major_fault))
            .legend(move |(x, y)| Rectangle::new([(x - 3, y - 3), (x + 3, y + 3)], Palette99::pick(2).filled()));
        contextarea.configure_series_labels()
            .border_style(BLACK)
            .background_style(WHITE.mix(0.7))
            .label_font((LABELS_STYLE_FONT, LABELS_STYLE_FONT_SIZE))
            .position(UpperLeft)
            .draw()
            .unwrap();
        /*
        // os load figure
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
        let high_value_load_1 = unlocked_historical_data.misc_details.iter()
            .filter(|((hostname, _), _)| hostname == filter_hostname)
            .map(|((_, _), row)| row.load_1)
            .max_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        let high_value_load_5 = unlocked_historical_data.misc_details.iter()
            .filter(|((hostname, _), _)| hostname == filter_hostname)
            .map(|((_, _), row)| row.load_5)
            .max_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        let high_value_load_15 = unlocked_historical_data.misc_details.iter()
            .filter(|((hostname, _), _)| hostname == filter_hostname)
            .map(|((_, _), row)| row.load_15)
            .max_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        let high_value = vec![high_value_load_1, high_value_load_5, high_value_load_15].into_iter().max_by(|a, b| a.partial_cmp(b).unwrap()).unwrap();
        let filename = format!("{filter_hostname}_load.png");

        let root = BitMapBackend::new(&filename, (1280,900)).into_drawing_area();
        root.fill(&WHITE).unwrap();
        let mut contextarea = ChartBuilder::on(&root)
            .set_label_area_size(LabelAreaPosition::Left, LABEL_AREA_SIZE_LEFT)
            .set_label_area_size(LabelAreaPosition::Bottom, LABEL_AREA_SIZE_BOTTOM)
            .set_label_area_size(LabelAreaPosition::Right, LABEL_AREA_SIZE_RIGHT)
            .caption(format!("load: {}",filter_hostname), (CAPTION_STYLE_FONT, CAPTION_STYLE_FONT_SIZE))
            .build_cartesian_2d(*start_time..*end_time, low_value..high_value)
            .unwrap();
        contextarea.configure_mesh()
            .x_labels(4)
            .x_label_formatter(&|x| x.to_rfc3339())
            .y_desc("load")
            .label_style((MESH_STYLE_FONT, MESH_STYLE_FONT_SIZE))
            .draw()
            .unwrap();

        let min_load_1 = unlocked_historical_data.misc_details.iter()
            .filter(|((hostname, _), _)| hostname == filter_hostname )
            .map(|((_, _), row)| row.load_1 )
            .min_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        let max_load_1 = unlocked_historical_data.misc_details.iter()
            .filter(|((hostname, _), _)| hostname == filter_hostname )
            .map(|((_, _), row)| row.load_1 )
            .max_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        contextarea.draw_series(LineSeries::new(unlocked_historical_data.misc_details.iter()
                                                    .filter(|((hostname, _), _)| hostname == filter_hostname)
                                                    .map(|((_, timestamp), row)| (*timestamp, row.load_1 )),
                                                Palette99::pick(1))
        )
            .unwrap()
            .label(format!("{:25} min: {:10.3}, max: {:10.3}", "load 1 min", min_load_1, max_load_1))
            .legend(move |(x, y)| Rectangle::new([(x - 3, y - 3), (x + 3, y + 3)], Palette99::pick(1).filled()));

        let min_load_5 = unlocked_historical_data.misc_details.iter()
            .filter(|((hostname, _), _)| hostname == filter_hostname )
            .map(|((_, _), row)| row.load_5 )
            .min_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        let max_load_5 = unlocked_historical_data.misc_details.iter()
            .filter(|((hostname, _), _)| hostname == filter_hostname )
            .map(|((_, _), row)| row.load_5 )
            .max_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        contextarea.draw_series(LineSeries::new(unlocked_historical_data.misc_details.iter()
                                                    .filter(|((hostname, _), _)| hostname == filter_hostname)
                                                    .map(|((_, timestamp), row)| (*timestamp, row.load_5 )),
                                                Palette99::pick(2))
        )
            .unwrap()
            .label(format!("{:25} min: {:10.3}, max: {:10.3}", "load 5 min", min_load_5, max_load_5))
            .legend(move |(x, y)| Rectangle::new([(x - 3, y - 3), (x + 3, y + 3)], Palette99::pick(2).filled()));

        let min_load_15 = unlocked_historical_data.misc_details.iter()
            .filter(|((hostname, _), _)| hostname == filter_hostname )
            .map(|((_, _), row)| row.load_15 )
            .min_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        let max_load_15 = unlocked_historical_data.misc_details.iter()
            .filter(|((hostname, _), _)| hostname == filter_hostname )
            .map(|((_, _), row)| row.load_15 )
            .max_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        contextarea.draw_series(LineSeries::new(unlocked_historical_data.misc_details.iter()
                                                    .filter(|((hostname, _), _)| hostname == filter_hostname)
                                                    .map(|((_, timestamp), row)| (*timestamp, row.load_15 )),
                                                Palette99::pick(3))
        )
            .unwrap()
            .label(format!("{:25} min: {:10.3}, max: {:10.3}", "load 15 min", min_load_15, max_load_15))
            .legend(move |(x, y)| Rectangle::new([(x - 3, y - 3), (x + 3, y + 3)], Palette99::pick(3).filled()));

        contextarea.configure_series_labels()
            .border_style(BLACK)
            .background_style(WHITE.mix(0.7))
            .label_font((LABELS_STYLE_FONT, LABELS_STYLE_FONT_SIZE))
            .position(UpperLeft)
            .draw()
            .unwrap();

         */
    }
}

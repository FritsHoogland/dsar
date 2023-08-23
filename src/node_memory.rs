use std::{collections::BTreeMap, sync::{Arc, Mutex}};
use prometheus_parse::{Value, Sample};
use itertools::Itertools;
use log::*;
use plotters::prelude::*;
use plotters::chart::SeriesLabelPosition::UpperLeft;

use crate::{Statistic, HistoricalData, CAPTION_STYLE_FONT, CAPTION_STYLE_FONT_SIZE, MESH_STYLE_FONT, MESH_STYLE_FONT_SIZE, LABELS_STYLE_FONT, LABELS_STYLE_FONT_SIZE, LABEL_AREA_SIZE_LEFT, LABEL_AREA_SIZE_BOTTOM, LABEL_AREA_SIZE_RIGHT};

#[derive(Debug)]
pub struct NodeMemoryDetails {
    pub active_anon: f64,
    pub active: f64,
    pub active_file: f64,
    pub anonhugepages: f64,
    pub anonpages: f64,
    pub bounce: f64,
    pub buffers: f64,
    pub cached: f64,
    pub commitlimit: f64,
    pub committed_as: f64,
    pub directmap2m: f64,
    pub directmap4k: f64,
    pub dirty: f64,
    pub filehugepages: f64,
    pub filepmdmapped: f64,
    pub hardwarecorrupted: f64,
    pub hugepages_free: f64,
    pub hugepages_rsvd: f64,
    pub hugepages_surp: f64,
    pub hugepages_total: f64,
    pub hugepagesize: f64,
    pub hugetlb: f64,
    pub inactive_anon: f64,
    pub inactive: f64,
    pub inactive_file: f64,
    pub kreclaimable: f64,
    pub kernelstack: f64,
    pub mapped: f64,
    pub memavailable: f64,
    pub memfree: f64,
    pub memtotal: f64,
    pub mlocked: f64,
    pub nfs_unstable: f64,
    pub pagetables: f64,
    pub percpu: f64,
    pub sreclaimable: f64,
    pub sunreclaim: f64,
    pub shmemhugepages: f64,
    pub shmempmdmapped: f64,
    pub shmem: f64,
    pub slab: f64,
    pub swapcached: f64,
    pub swapfree: f64,
    pub swaptotal: f64,
    pub unevictable: f64,
    pub vmallocchunk: f64,
    pub vmalloctotal: f64,
    pub vmallocused: f64,
    pub writebacktmp: f64,
    pub writeback: f64,
}


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
            row.first_value = false;
            debug!("{} last_value: {}, last_timestamp: {}", sample.metric, row.last_value, row.last_timestamp);
        })
        .or_insert(
            Statistic
            {
                last_value: value,
                last_timestamp: sample.timestamp,
                first_value: true,
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
        if statistics.iter().any(|((host, metric, _, _), _)| host == hostname && metric == "node_memory_MemFree_bytes")
        {
            let memory_free = statistics.iter().find(|((host, metric, _, _), _)| host == hostname && metric == "node_memory_MemFree_bytes").map(|((_, _, _, _), statistic)| statistic.last_value).unwrap();
            let memory_available = statistics.iter().find(|((host, metric, _, _), _)| host == hostname && metric == "node_memory_MemAvailable_bytes").map(|((_, _, _, _), statistic)| statistic.last_value).unwrap();
            let memory_total = statistics.iter().find(|((host, metric, _, _), _)| host == hostname && metric == "node_memory_MemTotal_bytes").map(|((_, _, _, _), statistic)| statistic.last_value).unwrap();
            let memory_used = memory_total - memory_free;
            let memory_buffers = statistics.iter().find(|((host, metric, _, _), _)| host == hostname && metric == "node_memory_Buffers_bytes").map(|((_, _, _, _), statistic)| statistic.last_value).unwrap();
            let memory_cached = statistics.iter().find(|((host, metric, _, _), _)| host == hostname && metric == "node_memory_Cached_bytes").map(|((_, _, _, _), statistic)| statistic.last_value).unwrap();
            let memory_commit = statistics.iter().find(|((host, metric, _, _), _)| host == hostname && metric == "node_memory_Committed_AS_bytes").map(|((_, _, _, _), statistic)| statistic.last_value).unwrap();
            let memory_swap_total = statistics.iter().find(|((host, metric, _, _), _)| host == hostname && metric == "node_memory_SwapTotal_bytes").map(|((_, _, _, _), statistic)| statistic.last_value).unwrap();
            let memory_active = statistics.iter().find(|((host, metric, _, _), _)| host == hostname && metric == "node_memory_Active_bytes").map(|((_, _, _, _), statistic)| statistic.last_value).unwrap();
            let memory_inactive = statistics.iter().find(|((host, metric, _, _), _)| host == hostname && metric == "node_memory_Inactive_bytes").map(|((_, _, _, _), statistic)| statistic.last_value).unwrap();
            let memory_dirty = statistics.iter().find(|((host, metric, _, _), _)| host == hostname && metric == "node_memory_Dirty_bytes").map(|((_, _, _, _), statistic)| statistic.last_value).unwrap();
            let memory_anonymous = statistics.iter().find(|((host, metric, _, _), _)| host == hostname && metric == "node_memory_AnonPages_bytes").map(|((_, _, _, _), statistic)| statistic.last_value).unwrap();
            let memory_slab = statistics.iter().find(|((host, metric, _, _), _)| host == hostname && metric == "node_memory_Slab_bytes").map(|((_, _, _, _), statistic)| statistic.last_value).unwrap();
            let memory_kernel_stack = statistics.iter().find(|((host, metric, _, _), _)| host == hostname && metric == "node_memory_KernelStack_bytes").map(|((_, _, _, _), statistic)| statistic.last_value).unwrap();
            let memory_pagetables = statistics.iter().find(|((host, metric, _, _), _)| host == hostname && metric == "node_memory_PageTables_bytes").map(|((_, _, _, _), statistic)| statistic.last_value).unwrap();
            let memory_swapcached = statistics.iter().find(|((host, metric, _, _), _)| host == hostname && metric == "node_memory_SwapCached_bytes").map(|((_, _, _, _), statistic)| statistic.last_value).unwrap();
            let memory_shared = statistics.iter().find(|((host, metric, _, _), _)| host == hostname && metric == "node_memory_Shmem_bytes").map(|((_, _, _, _), statistic)| statistic.last_value).unwrap();
            let memory_mapped = statistics.iter().find(|((host, metric, _, _), _)| host == hostname && metric == "node_memory_Mapped_bytes").map(|((_, _, _, _), statistic)| statistic.last_value).unwrap();
            let memory_hardware_corrupted = statistics.iter().find(|((host, metric, _, _), _)| host == hostname && metric == "node_memory_HardwareCorrupted_bytes").map(|((_, _, _, _), statistic)| statistic.last_value).unwrap_or_default();
            let memory_virtual_memory = statistics.iter().find(|((host, metric, _, _), _)| host == hostname && metric == "node_memory_VmallocTotal_bytes").map(|((_, _, _, _), statistic)| statistic.last_value).unwrap();
            let time = statistics.iter().find(|((host, metric, _, _), _)| host == hostname && metric == "node_memory_MemFree_bytes").map(|((_, _, _, _), statistic)| statistic.last_timestamp).unwrap();
            match mode
            {
                "normal" => {
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
                },
                "all" => {
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
                             memory_virtual_memory / (1024. * 1024.)
                    );
                },
                "relevant" => {
                    println!("{:30} {:8} {:9.0} {:9.0} {:9.0} {:9.0} {:9.0} {:9.0} {:9.0} {:9.0} {:9.0} {:9.0} {:9.0} {:9.0} {:9.0}",
                             hostname,
                             time.format("%H:%M:%S"),
                             memory_total / (1024. * 1024.),
                             memory_swapcached / (1024. * 1024.),
                             memory_kernel_stack / (1024. * 1024.),
                             memory_hardware_corrupted / (1024.*1024.),
                             memory_slab / (1024. * 1024.),
                             memory_pagetables / (1024. * 1024.),
                             memory_shared / (1024. * 1024.),
                             memory_dirty / (1024. * 1024.),
                             (memory_mapped - memory_shared).max(0.) / (1024. * 1024.),
                             (memory_cached - memory_mapped.max(memory_shared) - memory_dirty) / (1024. * 1024.),
                             memory_anonymous / (1024. * 1024.),
                             memory_free / (1024. * 1024.),
                             memory_available / (1024. * 1024.),
                    );
                },
                &_ => {},
            }
        }
    }
}

pub fn print_sar_r_header(
    mode: &str,
)
{
    match mode
    {
        "normal" => {
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
        },
        "all" => {
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
        },
        "relevant" => {
            println!("{:30} {:8} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9}",
                     "hostname",
                     "time",
                     "total",
                     "swpcached",
                     "kernelstk",
                     "hwcorrupt",
                     "slab",
                     "pagetbls",
                     "shared",
                     "dirty",
                     "mapped",
                     "cached",
                     "anon",
                     "free",
                     "avail",
            );
        },
        &_ => {},
    }
}

pub fn print_sar_s(
    statistics: &BTreeMap<(String, String, String, String), Statistic>,
)
{
    for hostname in statistics.iter().map(|((hostname, _, _, _), _)| hostname).unique()
    {
        if statistics.iter().any(|((host, metric, _, _), _)| host == hostname && metric == "node_memory_SwapFree_bytes")
        {
            let swap_free = statistics.iter().find(|((host, metric, _, _), _)| host == hostname && metric == "node_memory_SwapFree_bytes").map(|((_, _, _, _), statistic)| statistic.last_value).unwrap();
            let swap_total = statistics.iter().find(|((host, metric, _, _), _)| host == hostname && metric == "node_memory_SwapTotal_bytes").map(|((_, _, _, _), statistic)| statistic.last_value).unwrap();
            let swap_used = swap_total - swap_free;
            let swap_used_percent = (swap_used / swap_total).max(0.) * 100.;
            let swap_cached = statistics.iter().find(|((host, metric, _, _), _)| host == hostname && metric == "node_memory_SwapCached_bytes").map(|((_, _, _, _), statistic)| statistic.last_value).unwrap();
            let swap_cached_percent = swap_cached / swap_used * 100.;
            let time = statistics.iter().find(|((host, metric, _, _), _)| host == hostname && metric == "node_memory_SwapFree_bytes").map(|((_, _, _, _), statistic)| statistic.last_timestamp).unwrap();
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

pub fn create_memory_plots(
    historical_data: &Arc<Mutex<HistoricalData>>,
)
{
    let unlocked_historical_data = historical_data.lock().unwrap();
    for filter_hostname in unlocked_historical_data.memory_details.keys().map(|(hostname, _)| hostname).unique()
    {
        let filename = format!("{}_memory.png", filter_hostname);
        let root = BitMapBackend::new(&filename, (1280,1300)).into_drawing_area();
        root.fill(&WHITE).unwrap();
        let (upper, lower) = root.split_vertically(900);
        // set the plot specifics
        let start_time = unlocked_historical_data.memory_details
            .keys()
            .filter(|(hostname,_)| hostname == filter_hostname)
            .map(|(_, timestamp)| timestamp)
            .min()
            .unwrap();
        let end_time = unlocked_historical_data.memory_details
            .keys()
            .filter(|(hostname,_)| hostname == filter_hostname)
            .map(|(_, timestamp)| timestamp)
            .max()
            .unwrap();
        let low_value: f64 = 0.0;
        let high_value = unlocked_historical_data.memory_details.iter()
            .filter(|((hostname, _),_)| hostname == filter_hostname)
            .map(|((_, _), row)| (row.memtotal + row.memtotal * 0.1) / (1024. * 1024.))
            .max_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        let ((_, _), latest) = unlocked_historical_data.memory_details.iter()
            .filter(|((hostname, _), _)| hostname == filter_hostname)
            .max_by_key(|((timestamp, _), _)| timestamp)
            .unwrap();

        // create the memory plot
        let mut contextarea = ChartBuilder::on(&upper)
            .set_label_area_size(LabelAreaPosition::Left, LABEL_AREA_SIZE_LEFT)
            .set_label_area_size(LabelAreaPosition::Bottom, LABEL_AREA_SIZE_BOTTOM)
            .set_label_area_size(LabelAreaPosition::Right, LABEL_AREA_SIZE_RIGHT)
            .caption(format!("Memory usage: {}",filter_hostname), (CAPTION_STYLE_FONT, CAPTION_STYLE_FONT_SIZE))
            .build_cartesian_2d(*start_time..*end_time, low_value..high_value)
            .unwrap();
        contextarea.configure_mesh()
            .x_labels(4)
            .x_label_formatter(&|x| x.to_rfc3339())
            .y_desc("Memory MB")
            .label_style((MESH_STYLE_FONT, MESH_STYLE_FONT_SIZE))
            .draw()
            .unwrap();
        // memory total
        let min_memory_total = unlocked_historical_data.memory_details.iter()
            .filter(|((hostname, _), _)| hostname == filter_hostname)
            .map(|((_, _), row)| row.memtotal / (1024. * 1024.))
            .min_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        let max_memory_total = unlocked_historical_data.memory_details.iter()
            .filter(|((hostname, _), _)| hostname == filter_hostname)
            .map(|((_, _), row)| row.memtotal / (1024. * 1024.))
            .max_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        contextarea.draw_series(AreaSeries::new(unlocked_historical_data.memory_details.iter()
                                                    .filter(|((hostname, _), _)| hostname == filter_hostname)
                                                    .map(|((_, timestamp), row)| (*timestamp, row.memtotal / (1024. * 1024.))),
                                                0.0, Palette99::pick(1))
        )
            .unwrap()
            .label(format!("{:25} min: {:10.2}, max: {:10.2}, latest: {:10.2}", "memory total", min_memory_total, max_memory_total, latest.memtotal/ (1024. * 1024.)))
            .legend(move |(x, y)| Rectangle::new([(x - 3, y - 3), (x + 3, y + 3)], Palette99::pick(1).filled()));
        // swap cached + kernelstack + hardware corrupted + slab + pagetables + dirty + cached + anonymous + memfree 12
        let min_memory_swapcached = unlocked_historical_data.memory_details.iter()
            .filter(|((hostname, _), _)| hostname == filter_hostname)
            .map(|((_, _), row)| row.swapcached / (1024. * 1024.))
            .min_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        let max_memory_swapcached = unlocked_historical_data.memory_details.iter()
            .filter(|((hostname, _), _)| hostname == filter_hostname)
            .map(|((_, _), row)| row.swapcached / (1024. * 1024.))
            .max_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        contextarea.draw_series(AreaSeries::new(unlocked_historical_data.memory_details.iter()
                                                    .filter(|((hostname, _), _)| hostname == filter_hostname)
                                                    .map(|((_, timestamp), row)| (*timestamp, (row.swapcached + row.kernelstack + row.hardwarecorrupted + row.slab + row.pagetables + row.cached + row.anonpages + row.memfree) / (1024. * 1024.))),
                                                0.0, Palette99::pick(12))
        )
            .unwrap()
            .label(format!("{:25} min: {:10.2}, max: {:10.2}, latest: {:10.2}", "swap cached", min_memory_swapcached, max_memory_swapcached, latest.swapcached / (1024. * 1024.)))
            .legend(move |(x, y)| Rectangle::new([(x - 3, y - 3), (x + 3, y + 3)], Palette99::pick(12).filled()));
        // kernelstack + hardware corrupted + slab + pagetables + dirty + cached + anonymous + memfree 11
        let min_memory_kernelstack = unlocked_historical_data.memory_details.iter()
            .filter(|((hostname, _), _)| hostname == filter_hostname)
            .map(|((_, _), row)| row.kernelstack / (1024. * 1024.))
            .min_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        let max_memory_kernelstack = unlocked_historical_data.memory_details.iter()
            .filter(|((hostname, _), _)| hostname == filter_hostname)
            .map(|((_, _), row)| row.kernelstack / (1024. * 1024.))
            .max_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        contextarea.draw_series(AreaSeries::new(unlocked_historical_data.memory_details.iter()
                                                    .filter(|((hostname, _), _)| hostname == filter_hostname)
                                                    .map(|((_, timestamp), row)| (*timestamp, (row.kernelstack + row.hardwarecorrupted + row.slab + row.pagetables + row.cached + row.anonpages + row.memfree) / (1024. * 1024.))),
                                                0.0, Palette99::pick(11))
        )
            .unwrap()
            .label(format!("{:25} min: {:10.2}, max: {:10.2}, latest: {:10.2}", "kernel stack", min_memory_kernelstack, max_memory_kernelstack, latest.kernelstack / (1024. * 1024.)))
            .legend(move |(x, y)| Rectangle::new([(x - 3, y - 3), (x + 3, y + 3)], Palette99::pick(11).filled()));
        // hardware corrupted + slab + pagetables + dirty + cached + anonymous + memfree 10
        let min_memory_corrupted = unlocked_historical_data.memory_details.iter()
            .filter(|((hostname, _), _)| hostname == filter_hostname)
            .map(|((_, _), row)| row.hardwarecorrupted / (1024. * 1024.))
            .min_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        let max_memory_corrupted = unlocked_historical_data.memory_details.iter()
            .filter(|((hostname, _), _)| hostname == filter_hostname)
            .map(|((_, _), row)| row.hardwarecorrupted / (1024. * 1024.))
            .max_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        contextarea.draw_series(AreaSeries::new(unlocked_historical_data.memory_details.iter()
                                                    .filter(|((hostname, _), _)| hostname == filter_hostname)
                                                    .map(|((_, timestamp), row)| (*timestamp, (row.hardwarecorrupted + row.slab + row.pagetables + row.cached + row.anonpages + row.memfree) / (1024. * 1024.))),
                                                0.0, Palette99::pick(10))
        )
            .unwrap()
            .label(format!("{:25} min: {:10.2}, max: {:10.2}, latest: {:10.2}", "hardware corrupted", min_memory_corrupted, max_memory_corrupted, latest.hardwarecorrupted / (1024. * 1024.)))
            .legend(move |(x, y)| Rectangle::new([(x - 3, y - 3), (x + 3, y + 3)], Palette99::pick(10).filled()));
        // slab + pagetables + dirty + cached + anonymous + memfree 9
        let min_memory_slab = unlocked_historical_data.memory_details.iter()
            .filter(|((hostname, _), _)| hostname == filter_hostname)
            .map(|((_, _), row)| row.slab / (1024. * 1024.))
            .min_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        let max_memory_slab = unlocked_historical_data.memory_details.iter()
            .filter(|((hostname, _), _)| hostname == filter_hostname)
            .map(|((_, _), row)| row.slab / (1024. * 1024.))
            .max_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        contextarea.draw_series(AreaSeries::new(unlocked_historical_data.memory_details.iter()
                                                    .filter(|((hostname, _), _)| hostname == filter_hostname)
                                                    .map(|((_, timestamp), row)| (*timestamp, (row.slab + row.pagetables + row.cached + row.anonpages + row.memfree) / (1024. * 1024.))),
                                                0.0, Palette99::pick(9))
        )
            .unwrap()
            .label(format!("{:25} min: {:10.2}, max: {:10.2}, latest: {:10.2}", "slab", min_memory_slab, max_memory_slab, latest.slab / (1024. * 1024.)))
            .legend(move |(x, y)| Rectangle::new([(x - 3, y - 3), (x + 3, y + 3)], Palette99::pick(9).filled()));
        // pagetables 8
        // pagetables + shmem + cached + anonymous + memfree 8
        let min_memory_pagetables = unlocked_historical_data.memory_details.iter()
            .filter(|((hostname, _), _)| hostname == filter_hostname)
            .map(|((_, _), row)| row.pagetables / (1024. * 1024.))
            .min_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        let max_memory_pagetables = unlocked_historical_data.memory_details.iter()
            .filter(|((hostname, _), _)| hostname == filter_hostname)
            .map(|((_, _), row)| row.pagetables / (1024. * 1024.))
            .max_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        contextarea.draw_series(AreaSeries::new(unlocked_historical_data.memory_details.iter()
                                                    .filter(|((hostname, _), _)| hostname == filter_hostname)
                                                    .map(|((_, timestamp), row)| (*timestamp, (row.pagetables + row.cached + row.anonpages + row.memfree) / (1024. * 1024.))),
                                                0.0, Palette99::pick(8))
        )
            .unwrap()
            .label(format!("{:25} min: {:10.2}, max: {:10.2}, latest: {:10.2}", "pagetables", min_memory_pagetables, max_memory_pagetables, latest.pagetables / (1024. * 1024.)))
            .legend(move |(x, y)| Rectangle::new([(x - 3, y - 3), (x + 3, y + 3)], Palette99::pick(8).filled()));
        // dirty 7
        // cached + anonymous + memfree 7
        let min_memory_dirty = unlocked_historical_data.memory_details.iter()
            .filter(|((hostname, _), _)| hostname == filter_hostname)
            .map(|((_, _), row)| row.dirty / (1024. * 1024.))
            .min_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        let max_memory_dirty = unlocked_historical_data.memory_details.iter()
            .filter(|((hostname, _), _)| hostname == filter_hostname)
            .map(|((_, _), row)| row.dirty / (1024. * 1024.))
            .max_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        contextarea.draw_series(AreaSeries::new(unlocked_historical_data.memory_details.iter()
                                                    .filter(|((hostname, _), _)| hostname == filter_hostname)
                                                    .map(|((_, timestamp), row)| (*timestamp, (row.cached + row.anonpages + row.memfree) / (1024. * 1024.))),
                                                0.0, Palette99::pick(7))
        )
            .unwrap()
            .label(format!("{:25} min: {:10.2}, max: {:10.2}, latest: {:10.2}", "dirty", min_memory_dirty, max_memory_dirty, latest.dirty / (1024. * 1024.)))
            .legend(move |(x, y)| Rectangle::new([(x - 3, y - 3), (x + 3, y + 3)], Palette99::pick(7).filled()));
        // shared memory 6
        // first looked like being inside cached, now it seems not to
        let min_memory_shared = unlocked_historical_data.memory_details.iter()
            .filter(|((hostname, _), _)| hostname == filter_hostname)
            .map(|((_, _), row)| row.shmem / (1024. * 1024.))
            .min_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        let max_memory_shared = unlocked_historical_data.memory_details.iter()
            .filter(|((hostname, _), _)| hostname == filter_hostname)
            .map(|((_, _), row)| row.shmem / (1024. * 1024.))
            .max_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        contextarea.draw_series(AreaSeries::new(unlocked_historical_data.memory_details.iter()
                                                    .filter(|((hostname, _), _)| hostname == filter_hostname)
                                                    .map(|((_, timestamp), row)| (*timestamp, (row.cached - row.dirty + row.anonpages + row.memfree) / (1024. * 1024.))),
                                                0.0, Palette99::pick(6))
        )
            .unwrap()
            .label(format!("{:25} min: {:10.2}, max: {:10.2}, latest: {:10.2}", "shared memory", min_memory_shared, max_memory_shared, latest.shmem / (1024. * 1024.)))
            .legend(move |(x, y)| Rectangle::new([(x - 3, y - 3), (x + 3, y + 3)], Palette99::pick(6).filled()));
        // mapped  5
        // cached - dirty + anonymous + memfree 5
        let min_memory_mapped = unlocked_historical_data.memory_details.iter()
            .filter(|((hostname, _), _)| hostname == filter_hostname)
            .map(|((_, _), row)| (row.mapped - row.shmem).max(0.) / (1024. * 1024.))
            .min_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        let max_memory_mapped = unlocked_historical_data.memory_details.iter()
            .filter(|((hostname, _), _)| hostname == filter_hostname)
            .map(|((_, _), row)| (row.mapped - row.shmem).max(0.) / (1024. * 1024.))
            .max_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        contextarea.draw_series(AreaSeries::new(unlocked_historical_data.memory_details.iter()
                                                    .filter(|((hostname, _), _)| hostname == filter_hostname)
                                                    .map(|((_, timestamp), row)| (*timestamp, (row.cached - row.shmem - row.dirty + row.anonpages + row.memfree) / (1024. * 1024.))),
                                                0.0, Palette99::pick(5))
        )
            .unwrap()
            .label(format!("{:25} min: {:10.2}, max: {:10.2}, latest: {:10.2}", "mapped", min_memory_mapped, max_memory_mapped, (latest.mapped - latest.shmem).max(0.) / (1024. * 1024.)))
            .legend(move |(x, y)| Rectangle::new([(x - 3, y - 3), (x + 3, y + 3)], Palette99::pick(5).filled()));
        // cached 4
        // cached - mapped - dirty + anonymous + memfree 4
        // actual cached memory is without mapped and dirty memory.
        let min_memory_cached = unlocked_historical_data.memory_details.iter()
            .filter(|((hostname, _), _)| hostname == filter_hostname)
            .map(|((_, _), row)| (row.cached - row.mapped.max(row.shmem) - row.dirty) / (1024. * 1024.))
            .min_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        let max_memory_cached = unlocked_historical_data.memory_details.iter()
            .filter(|((hostname, _), _)| hostname == filter_hostname)
            .map(|((_, _), row)| (row.cached - row.mapped.max(row.shmem) - row.dirty) / (1024. * 1024.))
            .max_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        contextarea.draw_series(AreaSeries::new(unlocked_historical_data.memory_details.iter()
                                                    .filter(|((hostname, _), _)| hostname == filter_hostname)
                                                    .map(|((_, timestamp), row)| (*timestamp, (row.cached - row.mapped.max(row.shmem) - row.dirty + row.anonpages + row.memfree) / (1024. * 1024.))),
                                                0.0, Palette99::pick(4))
        )
            .unwrap()
            .label(format!("{:25} min: {:10.2}, max: {:10.2}, latest: {:10.2}", "cached", min_memory_cached, max_memory_cached, (latest.cached - latest.mapped.max(latest.shmem) - latest.dirty) / (1024. * 1024.)))
            .legend(move |(x, y)| Rectangle::new([(x - 3, y - 3), (x + 3, y + 3)], Palette99::pick(4).filled()));
        // anonymous 3
        // anonymous + memfree
        let min_memory_anonymous = unlocked_historical_data.memory_details.iter()
            .filter(|((hostname, _), _)| hostname == filter_hostname)
            .map(|((_, _), row)| row.anonpages / (1024. * 1024.))
            .min_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        let max_memory_anonymous = unlocked_historical_data.memory_details.iter()
            .filter(|((hostname, _), _)| hostname == filter_hostname)
            .map(|((_, _), row)| row.anonpages / (1024. * 1024.))
            .max_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        contextarea.draw_series(AreaSeries::new(unlocked_historical_data.memory_details.iter()
                                                    .filter(|((hostname, _), _)| hostname == filter_hostname)
                                                    .map(|((_, timestamp), row)| (*timestamp, (row.anonpages + row.memfree) / (1024. * 1024.))),
                                                0.0, Palette99::pick(3))
        )
            .unwrap()
            .label(format!("{:25} min: {:10.2}, max: {:10.2}, latest: {:10.2}", "anonymous", min_memory_anonymous, max_memory_anonymous, latest.anonpages / (1024. * 1024.)))
            .legend(move |(x, y)| Rectangle::new([(x - 3, y - 3), (x + 3, y + 3)], Palette99::pick(3).filled()));
        // memfree 2
        let min_memory_free = unlocked_historical_data.memory_details.iter()
            .filter(|((hostname, _), _)| hostname == filter_hostname)
            .map(|((_, _), row)| row.memfree / (1024. * 1024.))
            .min_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        let max_memory_free = unlocked_historical_data.memory_details.iter()
            .filter(|((hostname, _), _)| hostname == filter_hostname)
            .map(|((_, _), row)| row.memfree / (1024. * 1024.))
            .max_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        contextarea.draw_series(AreaSeries::new(unlocked_historical_data.memory_details.iter()
                                                    .filter(|((hostname, _), _)| hostname == filter_hostname)
                                                    .map(|((_, timestamp), row)| (*timestamp, row.memfree / (1024. * 1024.))),
                                                0.0, Palette99::pick(2))
        )
            .unwrap()
            .label(format!("{:25} min: {:10.2}, max: {:10.2}, latest: {:10.2}", "memory free", min_memory_free, max_memory_free, latest.memfree / (1024. * 1024.)))
            .legend(move |(x, y)| Rectangle::new([(x - 3, y - 3), (x + 3, y + 3)], Palette99::pick(2).filled()));
        // available memory: line - RED
        let min_memory_available = unlocked_historical_data.memory_details.iter()
            .filter(|((hostname, _), _)| hostname == filter_hostname)
            .map(|((_, _), row)| row.memavailable / (1024. * 1024.))
            .min_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        let max_memory_available = unlocked_historical_data.memory_details.iter()
            .filter(|((hostname, _), _)| hostname == filter_hostname)
            .map(|((_, _), row)| row.memavailable / (1024. * 1024.))
            .max_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        contextarea.draw_series(LineSeries::new(unlocked_historical_data.memory_details.iter()
                                                    .filter(|((hostname, _), _)| hostname == filter_hostname)
                                                    .map(|((_, timestamp), row)| (*timestamp, row.memavailable / (1024. * 1024.))),
                                                ShapeStyle { color: RED.into(), filled: false, stroke_width: 2} )
        )
            .unwrap()
            .label(format!("{:25} min: {:10.2}, max: {:10.2}, latest: {:10.2}", "memory available", min_memory_available, max_memory_available, latest.memavailable / (1024. * 1024.)))
            .legend(move |(x, y)| Rectangle::new([(x - 3, y - 3), (x + 3, y + 3)], RED.filled()));
        contextarea.configure_series_labels()
            .border_style(BLACK)
            .background_style(WHITE.mix(0.7))
            .label_font((LABELS_STYLE_FONT, LABELS_STYLE_FONT_SIZE))
            .position(UpperLeft)
            .draw()
            .unwrap();

        // create the swap plot
        let low_swap_value: f64 = 0.0;
        let high_swap_value = unlocked_historical_data.memory_details.iter()
            .filter(|((hostname, _),_)| hostname == filter_hostname)
            .map(|((_, _), row)| (row.swaptotal) / (1024. * 1024.))
            .max_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();

        let mut contextarea = ChartBuilder::on(&lower)
            .set_label_area_size(LabelAreaPosition::Left, LABEL_AREA_SIZE_LEFT)
            .set_label_area_size(LabelAreaPosition::Bottom, LABEL_AREA_SIZE_BOTTOM)
            .set_label_area_size(LabelAreaPosition::Right, LABEL_AREA_SIZE_RIGHT)
            .caption(format!("Swap usage: {}",filter_hostname), (CAPTION_STYLE_FONT, CAPTION_STYLE_FONT_SIZE))
            .build_cartesian_2d(*start_time..*end_time, low_swap_value..high_swap_value)
            .unwrap();
        contextarea.configure_mesh()
            .x_labels(4)
            .x_label_formatter(&|x| x.to_rfc3339())
            .y_desc("Swap MB")
            .label_style((MESH_STYLE_FONT, MESH_STYLE_FONT_SIZE))
            .draw()
            .unwrap();
        // swap total
        let min_swap_total = unlocked_historical_data.memory_details.iter()
            .filter(|((hostname, _), _)| hostname == filter_hostname)
            .map(|((_, _), row)| row.swaptotal / (1024. * 1024.))
            .min_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        let max_swap_total = unlocked_historical_data.memory_details.iter()
            .filter(|((hostname, _), _)| hostname == filter_hostname)
            .map(|((_, _), row)| row.swaptotal / (1024. * 1024.))
            .max_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        contextarea.draw_series(AreaSeries::new(unlocked_historical_data.memory_details.iter()
                                                    .filter(|((hostname, _), _)| hostname == filter_hostname)
                                                    .map(|((_, timestamp), row)| (*timestamp, row.swaptotal / (1024. * 1024.))),
                                                0.0, Palette99::pick(1))
        )
            .unwrap()
            .label(format!("{:25} min: {:10.2}, max: {:10.2}, latest: {:10.2}", "swap total", min_swap_total, max_swap_total, latest.swaptotal / (1024. * 1024.)))
            .legend(move |(x, y)| Rectangle::new([(x - 3, y - 3), (x + 3, y + 3)], Palette99::pick(1).filled()));
        let min_swap_used = unlocked_historical_data.memory_details.iter()
            .filter(|((hostname, _), _)| hostname == filter_hostname)
            .map(|((_, _), row)| (row.swaptotal - row.swapfree) / (1024. * 1024.))
            .min_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        let max_swap_used = unlocked_historical_data.memory_details.iter()
            .filter(|((hostname, _), _)| hostname == filter_hostname)
            .map(|((_, _), row)| (row.swaptotal - row.swapfree) / (1024. * 1024.))
            .max_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        contextarea.draw_series(AreaSeries::new(unlocked_historical_data.memory_details.iter()
                                                    .filter(|((hostname, _), _)| hostname == filter_hostname)
                                                    .map(|((_, timestamp), row)| (*timestamp, (row.swaptotal - row.swapfree) / (1024. * 1024.))),
                                                0.0, Palette99::pick(2))
        )
            .unwrap()
            .label(format!("{:25} min: {:10.2}, max: {:10.2}, latest: {:10.2}", "swap used", min_swap_used, max_swap_used, (latest.swaptotal - latest.swapfree) / (1024. * 1024.)))
            .legend(move |(x, y)| Rectangle::new([(x - 3, y - 3), (x + 3, y + 3)], Palette99::pick(2).filled()));
        contextarea.configure_series_labels()
            .border_style(BLACK)
            .background_style(WHITE.mix(0.7))
            .label_font((LABELS_STYLE_FONT, LABELS_STYLE_FONT_SIZE))
            .position(UpperLeft)
            .draw()
            .unwrap();
    }
}

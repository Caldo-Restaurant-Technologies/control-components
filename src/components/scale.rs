use crate::components::load_cell::LoadCell;
use log::info;
use std::array;
use std::error::Error;
use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::sleep;
use tokio::sync::mpsc::error::TryRecvError;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::sync::oneshot;
use tokio::time::{Duration, Instant, MissedTickBehavior};

pub type DiagnoseResult = Result<(Scale, Vec<Duration>, Vec<f64>), Box<dyn Error + Send + Sync>>;
pub struct Scale {
    cells: [LoadCell; 4],
    cell_coefficients: Vec<f64>,
    tare_offset: f64,
    connected: bool,
}

impl Scale {
    pub fn new(phidget_id: i32) -> Self {
        let cells: [LoadCell; 4] = array::from_fn(|i| LoadCell::new(phidget_id, i as i32));
        Self {
            cells,
            cell_coefficients: vec![0.; 4],
            tare_offset: 0.,
            connected: false,
        }
    }

    pub fn actor_tx_pair(
        self,
    ) -> (
        Sender<ScaleCmd>,
        impl Future<Output = Result<(), Box<dyn Error + Send + Sync>>>,
    ) {
        let (tx, rx) = channel(100);
        (tx, actor(self, rx))
    }

    pub fn connect(mut self) -> Result<Self, Box<dyn Error + Send + Sync>> {
        for cell in 0..self.cells.len() {
            self.cells[cell].connect()?;
        }
        self.connected = true;
        Ok(self)
    }

    fn get_readings(scale: Self) -> Result<(Self, Vec<f64>), Box<dyn Error + Send + Sync>> {
        // Gets each load cell reading from Phidget
        // and returns them in a matrix.
        let readings: Vec<f64> = scale
            .cells
            .as_slice()
            .iter()
            .map(|cell| cell.get_reading().unwrap())
            .collect();
        Ok((scale, readings))
    }

    pub fn live_weigh(mut scale: Self) -> Result<(Self, f64), Box<dyn Error + Send + Sync>> {
        // Gets the instantaneous weight measurement
        // from the scale by taking the sum of each
        // load cell's reading, weighted by its
        // coefficient.
        let readings: Vec<f64>;
        (scale, readings) = Scale::get_readings(scale)?;
        let weight = dot(readings, scale.cell_coefficients.clone()) - scale.tare_offset;
        Ok((scale, weight))
    }

    pub fn weight_by_median(
        mut scale: Self,
        time: Duration,
        sample_rate: usize,
    ) -> Result<(Self, f64), Box<dyn Error + Send + Sync>> {
        let mut weights = Vec::new();
        let delay = Duration::from_secs_f64(1. / sample_rate as f64);
        let start_time = Instant::now();
        scale = loop {
            if Instant::now() - start_time > time {
                break scale;
            }
            let weight: f64;
            (scale, weight) = Scale::live_weigh(scale)?;
            weights.push(weight);
            sleep(delay);
        };
        Ok((scale, Scale::median(&mut weights)))
    }

    fn median(weights: &mut [f64]) -> f64 {
        weights.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let middle = weights.len() / 2;
        weights[middle]
    }

    pub fn get_medians(scale: Self, time: Duration, sample_rate: f64) -> (Self, Vec<f64>) {
        let mut readings: Vec<Vec<f64>> = vec![vec![]; 4];
        let mut medians = vec![0.; 4];
        let delay = Duration::from_secs_f64(1. / sample_rate);
        let start_time = Instant::now();
        loop {
            let curr_time = Instant::now();
            if curr_time - start_time > time {
                break;
            }

            for (idx, cell) in scale.cells.iter().enumerate().take(scale.cells.len()) {
                readings[idx].push(cell.get_reading().expect("Failed to get reading"))
            }
            sleep(delay);
        }
        for cell in 0..scale.cells.len() {
            medians[cell] = Scale::median(&mut readings[cell]);
        }
        (scale, medians)
    }

    pub fn change_coefficients(mut scale: Self, coefficients: Vec<f64>) -> Self {
        scale.cell_coefficients = coefficients;
        scale
    }

    pub fn diagnose(mut scale: Self, duration: Duration, sample_rate: usize) -> DiagnoseResult {
        let mut times = Vec::new();
        let mut weights = Vec::new();
        let data_interval = Duration::from_secs_f64(1. / sample_rate as f64);
        let init_time = Instant::now();

        scale = loop {
            if Instant::now() - init_time > duration {
                break scale;
            }
            let weight: f64;
            (scale, weight) = Scale::live_weigh(scale)?;
            let time = Instant::now() - init_time;
            times.push(time);
            weights.push(weight);
            sleep(data_interval);
        };

        Ok((scale, times, weights))
    }
}

fn dot(vec1: Vec<f64>, vec2: Vec<f64>) -> f64 {
    assert_eq!(vec1.len(), vec2.len());
    let mut sum = 0.;
    for elem in 0..vec1.len() {
        sum += vec1[elem] * vec2[elem];
    }
    sum
}

pub struct ScaleCmd(pub oneshot::Sender<f64>);

pub async fn actor(
    mut scale: Scale,
    mut receiver: Receiver<ScaleCmd>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let mut tick_interval = tokio::time::interval(Duration::from_millis(10));
    tick_interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let shutdown = Arc::new(AtomicBool::new(false));
    signal_hook::flag::register(signal_hook::consts::SIGINT, Arc::clone(&shutdown))
        .expect("Register hook");

    loop {
        if shutdown.load(Ordering::Relaxed) {
            drop(scale);
            break;
        }
        let weight: f64;
        (scale, weight) = tokio::task::spawn_blocking(move || Scale::live_weigh(scale).unwrap())
            .await
            .unwrap();

        match receiver.try_recv() {
            Ok(cmd) => {
                // info!("Read weight: {weight}");
                cmd.0.send(weight).unwrap()
            }
            Err(TryRecvError::Disconnected) => {
                info!("All senders dropped, Disconnecting");
                break;
            }
            Err(_) => {}
        }
        tick_interval.tick().await;
    }
    Ok(())
}

#[test]
fn calibrate() {
    let mut scale = Scale::new(716623);
    scale = scale.connect().unwrap();
    let (_scale, readings) = Scale::get_medians(scale, Duration::from_secs(1), 50.);
    println!("Cell Medians: {:?}", readings)
}

#[test]
fn connect_scale_cells() -> Result<(), Box<dyn Error + Send + Sync>> {
    let scale = Scale::new(716709);
    Scale::connect(scale)?;
    Ok(())
}

#[test]
fn read_scale() -> Result<(), Box<dyn Error + Send + Sync>> {
    let mut scale = Scale::new(716709);
    scale = Scale::connect(scale)?;
    let (_scale, readings) = Scale::get_readings(scale)?;
    println!("Scale Readings: {:?}", readings);
    Ok(())
}

#[test]
fn live_weigh_scale() -> Result<(), Box<dyn Error + Send + Sync>> {
    let mut scale = Scale::new(716709);
    scale = Scale::connect(scale)?;
    let (_, weight) = Scale::live_weigh(scale)?;
    println!("Weight: {:?}", weight);

    Ok(())
}

#[test]
fn weigh_scale() -> Result<(), Box<dyn Error + Send + Sync>> {
    let mut scale = Scale::new(716623);
    scale = Scale::connect(scale)?;
    // scale = Scale::change_coefficients(scale, vec![-4926943.639406107, 2486765.6938639805, -4985950.215221712, 4799388.712869379]);
    scale = Scale::change_coefficients(
        scale,
        vec![
            4780449.913365008,
            2596299.373482612,
            -4975764.006916862,
            4998589.065848139,
        ],
    );
    let (_, weight) = Scale::weight_by_median(scale, Duration::from_secs(3), 50)?;
    println!("Weight: {:?}", weight - 4268.);

    Ok(())
}

#[test]
fn test_dot() {
    let vec1 = vec![1., 2., 3., 4.];
    let vec2 = vec![1., 1., 1., 0.];
    assert_eq!(dot(vec1, vec2), 6.);
}

#[test]
fn test_median() {
    let mut arr = vec![0., 6., 1., 3., 4.];
    let ans = Scale::median(&mut arr);
    assert_eq!(ans, 3.);
}
//
// #[test]
// fn calibrate_scale() -> Result<(), Box<dyn Error>> {
//     let mut scale = Scale::new(716709);
//     scale.connect()?;
//     scale.calibrate(437.7, 1000, 100)?;
//
//     Ok(())
// }
//
// #[test]
// fn get_medians() -> Result<(), Box<dyn Error>> {
//     let mut scale = Scale::new(716709);
//     scale.connect()?;
//     let medians = scale.get_medians(1000, 50)?;
//     println!("Medians: {:?}", medians);
//     Ok(())
// }
//
// #[test]
// fn diagnose_scale() -> Result<(), Box<dyn Error>> {
//     let mut scale = Scale::new(716709);
//     scale.connect()?;
//     let (_times, _weights) = scale.diagnose(Duration::from_secs(5), 100)?;
//     Ok(())
// }

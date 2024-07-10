use crate::components::clear_core_motor::ClearCoreMotor;
use crate::components::scale::Scale;
use std::error::Error;
use serde::Deserialize;
use tokio::sync::mpsc::Receiver;
use tokio::sync::oneshot;
use tokio::time::{Duration, Instant};
use crate::interface::tcp::client;

#[derive(Deserialize)]
pub struct DispensingParameters {
    serving_weight: Option<f64>,
    timeout: Option<Duration>,
    motor_speed: f64,
    sample_rate: f64,
    cutoff_frequency: f64,
    check_offset: f64,
    stop_offset: f64,
}
impl DispensingParameters {
    pub fn with_weight(
        serving_weight: f64,
        timeout: Duration,
        motor_speed: f64,
        sample_rate: f64,
        cutoff_frequency: f64,
        check_offset: f64,
        stop_offset: f64,
    ) -> Self {
        Self {
            serving_weight: Some(serving_weight),
            timeout: Some(timeout),
            motor_speed,
            sample_rate,
            cutoff_frequency,
            check_offset,
            stop_offset,
        }
    }
    pub fn only_timeout(
        timeout: Duration,
        motor_speed: f64,
        sample_rate: f64,
        cutoff_frequency: f64,
        check_offset: f64,
        stop_offset: f64,
    ) -> Self {
        Self {
            serving_weight: None,
            timeout: Some(timeout),
            motor_speed,
            sample_rate,
            cutoff_frequency,
            check_offset,
            stop_offset,
        }
    }
}

pub struct Node {
    motor: ClearCoreMotor,
}

impl Node {
    pub fn new(motor: ClearCoreMotor) -> Self {
        Self { motor }
    }

    pub async fn connect_scale(&self, scale: Scale) -> Scale {
        tokio::task::spawn_blocking(move || Scale::connect(scale).expect("Scale failed to connect"))
            .await
            .unwrap()
    }

    pub async fn read_scale(&self, scale: Scale) -> (Scale, f64) {
        tokio::task::spawn_blocking(move || {
            Scale::live_weigh(scale).expect("Scale failed to weigh")
        })
        .await
        .unwrap()
    }

    pub async fn read_scale_median(
        &self,
        scale: Scale,
        time: Duration,
        sample_rate: usize,
    ) -> (Scale, f64) {
        tokio::task::spawn_blocking(move || {
            Scale::weight_by_median(scale, time, sample_rate).expect("Failed to weigh scale")
        })
        .await
        .unwrap()
    }

    pub async fn dispense(
        &self,
        scale: Scale,
        parameters: DispensingParameters, // serving: f64,
                                          // sample_rate: f64,
                                          // cutoff_frequency: f64,
                                          // motor_speed: f64,
    ) -> (Scale, Vec<Duration>, Vec<f64>) {
        // Prime conveyor
        self.motor
            .set_velocity(2. * parameters.motor_speed)
            .await
            .unwrap();
        // self.motor.relative_move(-10000.).await.unwrap();

        // Set LP filter values
        let filter_period = 1. / parameters.sample_rate;
        let filter_rc = 1. / (parameters.cutoff_frequency * 2. * std::f64::consts::PI);
        let filter_a = filter_period / (filter_period + filter_rc);
        let filter_b = filter_rc / (filter_period + filter_rc);

        // Initialize dispense tracking variables
        let init_time = Instant::now();
        let mut last_sent_motor = Instant::now();

        let (mut scale, init_weight) = self
            .read_scale_median(scale, Duration::from_secs(3), 50)
            .await;

        let mut curr_weight = init_weight;
        let target_weight = init_weight - parameters.serving_weight.unwrap();
        let mut reading: f64;
        let mut final_weight: f64;

        let timeout = Duration::from_secs(90);
        let send_command_delay = Duration::from_millis(500);

        let mut times: Vec<Duration> = Vec::new();
        let mut weights: Vec<f64> = Vec::new();

        self.motor
            .set_velocity(parameters.motor_speed)
            .await
            .expect("Failed to change velocity");
        self.motor
            .relative_move(100.)
            .await
            .expect("Failed to send move command");
        let (scale, dispensed) = loop {
            if curr_weight < target_weight - parameters.check_offset {
                self.motor.abrupt_stop().await.expect("Failed to stop");
                (scale, final_weight) = self
                    .read_scale_median(scale, Duration::from_secs(2), 50)
                    .await;
                if final_weight <= target_weight - parameters.stop_offset {
                    break (scale, init_weight - final_weight);
                }
            }
            let curr_time = Instant::now();
            if curr_time - init_time > timeout {
                // TODO: maybe violently run in reverse for a couple seconds and let it keep running?
                self.motor.abrupt_stop().await.expect("Failed to stop");
                println!("WARNING: Dispense timed out!");
                break (scale, init_weight - curr_weight);
            }
            (scale, reading) = self.read_scale(scale).await;
            curr_weight = filter_a * reading + filter_b * curr_weight;

            times.push(curr_time - init_time);
            weights.push(reading);

            if curr_time - last_sent_motor > send_command_delay {
                last_sent_motor = Instant::now();
                let err = (curr_weight - target_weight) / parameters.serving_weight.unwrap();
                let new_motor_speed = err * parameters.motor_speed;
                if new_motor_speed >= 0.1 {
                    self.motor
                        .set_velocity(new_motor_speed)
                        .await
                        .expect("Failed to change speed");
                }
                self.motor
                    .relative_move(100.0)
                    .await
                    .expect("Failed to update");
            }
        };
        println!("Dispensed: {:.1} g", dispensed);
        (scale, times, weights)
    }
    //
    pub async fn timed_dispense(&self, scale: Scale, parameters: DispensingParameters) -> Scale {
        // Set LP filter values
        let filter_period = 1. / parameters.sample_rate;
        let filter_rc = 1. / (parameters.cutoff_frequency * 2. * std::f64::consts::PI);
        let filter_a = filter_period / (filter_period + filter_rc);
        let filter_b = filter_rc / (filter_period + filter_rc);

        // Initialize dispense tracking variables
        let init_time = Instant::now();
        let mut last_sent_motor = Instant::now();

        let (mut scale, init_weight) = self
            .read_scale_median(scale, Duration::from_secs(3), 200)
            .await;

        let mut curr_weight = init_weight;
        let mut reading: f64;
        let send_command_delay = Duration::from_millis(250);

        // Data tracking
        let mut times = Vec::new();
        let mut weights = Vec::new();
        self.motor
            .set_velocity(parameters.motor_speed)
            .await
            .expect("TODO: panic message");
        self.motor
            .relative_move(100.0)
            .await
            .expect("Failed to update");
        loop {
            let curr_time = Instant::now();
            if curr_time - init_time > parameters.timeout.unwrap() {
                self.motor.abrupt_stop().await.expect("Failed to stop");
                break;
            }
            (scale, reading) = self.read_scale(scale).await;
            curr_weight = filter_a * reading + filter_b * curr_weight;

            times.push(curr_time - init_time);
            weights.push(curr_weight);

            if curr_time - last_sent_motor > send_command_delay {
                last_sent_motor = Instant::now();
                self.motor
                    .relative_move(10.0)
                    .await
                    .expect("Failed to update");
            }
        }

        let (scale, final_weight) = self
            .read_scale_median(scale, Duration::from_secs(3), 200)
            .await;
        println!("Dispensed: {:.1} g", init_weight - final_weight);
        scale
    }
    pub async fn actor(
        &self,
        phidget_id: i32,
        mut rx: Receiver<NodeCommand>,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let mut scale = self.connect_scale(Scale::new(phidget_id)).await;
        scale = Scale::change_coefficients(scale, vec![-5897877.72181665, 5263019.161459, -4005678.071311, 4000763.38549006]);
        self.motor.enable().await.unwrap();
        while let Some(cmd) = rx.recv().await {
            match cmd {
                NodeCommand::Dispense(p) => {
                    if p.serving_weight.is_some() {
                        (scale, _, _) = self.dispense(scale, p).await;
                    } else {
                        scale = self.timed_dispense(scale, p).await;
                    }
                }
                NodeCommand::ReadScale(sender) => {
                    let weight: f64;
                    (scale, weight) = self.read_scale(scale).await;
                    sender.send(weight).unwrap();
                }
                NodeCommand::ReadScaleMedian(sender) => {
                    let weight: f64;
                    (scale, weight) = self
                        .read_scale_median(scale, Duration::from_secs(2), 50)
                        .await;
                    sender.send(weight).unwrap();
                }
            }
        }
        Ok(())
    }
}

pub enum NodeCommand {
    Dispense(DispensingParameters),
    ReadScale(oneshot::Sender<f64>),
    ReadScaleMedian(oneshot::Sender<f64>),
}

#[tokio::test]
async fn test() {
    let (tx, rx) = tokio::sync::mpsc::channel(10);
    let (ntx, nrx) = tokio::sync::mpsc::channel(10);
    let cc1_handler = tokio::spawn(client("192.168.1.12:8888", rx));
    let node_handler = tokio::spawn(async move {
        let node = Node::new(ClearCoreMotor::new(3, 800, tx));
        node.actor(716620, nrx).await.expect("TODO: panic message");
    });

    let weigh = tokio::spawn( async move {
        let (rep_tx, rep_rx) = oneshot::channel();
        let msg = NodeCommand::ReadScaleMedian(rep_tx);
        ntx.send(msg).await.unwrap();
        let rep = rep_rx.await.unwrap();
        println!("Weight reading: {:.1}", rep-5383.);
    });

    let (_, _, _) = tokio::join!(weigh, node_handler, cc1_handler);
}


















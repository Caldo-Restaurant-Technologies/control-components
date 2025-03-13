use log::info;
use phidget::{devices::VoltageRatioInput, Phidget};
use std::error::Error;
use std::thread::sleep;
use std::time::Duration;
use tokio::time::Instant;

const TIMEOUT: Duration = phidget::TIMEOUT_DEFAULT;

pub struct LoadCell {
    phidget_id: i32,
    channel_id: i32,
    vin: VoltageRatioInput,
}
impl LoadCell {
    pub fn new(phidget_id: i32, channel_id: i32) -> Self {
        let vin = VoltageRatioInput::new();
        Self {
            phidget_id,
            channel_id,
            vin,
        }
    }

    pub fn connect(&mut self) -> Result<(), Box<dyn Error + Send + Sync>> {
        self.vin.set_serial_number(self.phidget_id)?;
        self.vin.set_channel(self.channel_id)?;
        self.vin.open_wait(TIMEOUT)?;
        let min_data_interval = self.vin.min_data_interval()?;
        self.vin.set_data_interval(min_data_interval)?;
        sleep(Duration::from_millis(3000));
        info!(
            "Channel {:} set for Phidget {:}",
            self.channel_id, self.phidget_id
        );
        Ok(())
    }

    pub fn get_reading(&self) -> Result<f64, Box<dyn Error + Send + Sync>> {
        // Gets the reading of a load cell from
        // Phidget.
        let reading = self.vin.voltage_ratio()?;
        Ok(reading)
    }

    pub fn diagnose(
        &self,
        duration: Duration,
        sample_rate: usize,
    ) -> Result<(Vec<Duration>, Vec<f64>), Box<dyn Error + Send + Sync>> {
        let mut times = Vec::new();
        let mut readings = Vec::new();
        let data_interval = Duration::from_secs_f64(1. / (sample_rate as f64));

        let init_time = Instant::now();
        while Instant::now() - init_time < duration {
            readings.push(self.get_reading()?);
            times.push(Instant::now() - init_time);
            sleep(data_interval);
        }
        Ok((times, readings))
    }
}

#[test]
fn get_load_cell_reading() {
    let mut cell = LoadCell::new(716709, 0);
    cell.connect().expect("Failed to connect load cell");
    let _reading = cell.get_reading().expect("Failed to read load cell");
}

#[test]
fn diagnose_load_cell() {
    let mut cell = LoadCell::new(716709, 0);
    cell.connect().expect("Failed to connect load cell");
    let (_times, _readings) = cell
        .diagnose(Duration::from_millis(500), 100)
        .expect("Failed to diagnose load cell");
}

use crate::components::send_recv::SendRecv;
use crate::subsystems::linear_actuator::Message;
use crate::util::utils::{ascii_to_int, make_prefix, num_to_bytes};
use log::error;
use serde::Serialize;
use std::result::Result;
pub use std::time::Duration;
use tokio::sync::mpsc::Sender;
use tokio::time::MissedTickBehavior;

const REPLY_IDX: usize = 3;
const _SUCCESSFUL_REPLY: u8 = b'_';
const FAILED_REPLY: u8 = b'?';

#[derive(Debug, PartialOrd, PartialEq, Serialize)]
pub enum Status {
    Disabled,
    Enabling,
    Faulted,
    Ready,
    Moving,
    Unknown,
}

#[derive(Clone)]
pub struct ClearCoreMotor {
    id: u8,
    prefix: [u8; 3],
    scale: usize,
    drive_sender: Sender<Message>,
}

impl ClearCoreMotor {
    pub fn new(id: u8, scale: usize, drive_sender: Sender<Message>) -> Self {
        let prefix = make_prefix(b'M', id);
        ClearCoreMotor {
            id,
            prefix,
            scale,
            drive_sender,
        }
    }

    async fn check_reply(&self, reply: &[u8]) -> Result<(), Status> {
        if reply[REPLY_IDX] == FAILED_REPLY {
            error!(
                "Response from motor controller: {:?}",
                String::from_utf8(reply.to_vec())
            );
            Err(self.get_status().await)
        } else {
            Ok(())
        }
    }

    pub async fn enable(&self) -> Result<&Self, Status> {
        let enable_cmd = [2, b'M', self.id + 48, b'E', b'N', 13];
        log::debug!("Enabled motor id: {}", self.id);
        let resp = self.write(enable_cmd.as_ref()).await;
        if let Err(err) = self.check_reply(resp.as_slice()).await {
            Err(err)
        } else {
            Ok(self)
        }
    }

    pub async fn disable(&self) {
        let enable_cmd = [2, b'M', self.id + 48, b'D', b'E', 13];
        log::debug!("Disabled motor id: {}", self.id);
        self.write(enable_cmd.as_ref()).await;
    }

    pub async fn absolute_move(&self, position: f64) -> Result<(), Status> {
        let position = num_to_bytes((position * (self.scale as f64)).trunc() as isize);
        let mut msg: Vec<u8> = Vec::with_capacity(position.len() + self.prefix.len() + 1);
        msg.extend_from_slice(self.prefix.as_slice());
        msg.extend_from_slice(b"AM");
        msg.extend_from_slice(position.as_slice());
        msg.push(13);
        let resp = self.write(msg.as_slice()).await;
        self.check_reply(&resp).await
    }

    pub async fn relative_move(&self, position: f64) -> Result<(), Status> {
        log::debug!("Called rel move on motor id: {}", self.id);
        let position = num_to_bytes((position * (self.scale as f64)).trunc() as isize);
        let mut msg: Vec<u8> = Vec::with_capacity(position.len() + self.prefix.len() + 1);
        msg.extend_from_slice(self.prefix.as_slice());
        msg.extend_from_slice(b"RM");
        msg.extend_from_slice(position.as_slice());
        msg.push(13);
        let resp = self.write(msg.as_slice()).await;
        self.check_reply(&resp).await
    }

    pub async fn jog(&self, speed: f64) -> Result<(), Status> {
        let speed = num_to_bytes((speed * (self.scale as f64)).trunc() as isize);
        let mut msg: Vec<u8> = Vec::with_capacity(speed.len() + self.prefix.len() + 1);
        msg.extend_from_slice(self.prefix.as_slice());
        msg.extend_from_slice(b"JG");
        msg.extend_from_slice(speed.as_slice());
        msg.push(13);
        let resp = self.write(msg.as_slice()).await;
        self.check_reply(&resp).await
    }

    pub async fn abrupt_stop(&self) {
        let stop_cmd = [2, b'M', self.id + 48, b'A', b'S', 13];
        log::debug!("Called abrupt stop on motor id: {}", self.id);
        self.write(stop_cmd.as_ref()).await;
    }

    pub async fn stop(&self) {
        let stop_cmd = [2, b'M', self.id + 48, b'S', b'T', 13];
        log::debug!("Called abrupt stop on motor id: {}", self.id);
        self.write(stop_cmd.as_ref()).await;
    }

    pub async fn set_position(&self, position: isize) {
        let pos = num_to_bytes(position * self.scale as isize);
        let mut msg: Vec<u8> = Vec::with_capacity(pos.len() + self.prefix.len() + 1);
        msg.extend_from_slice(self.prefix.as_slice());
        msg.extend_from_slice(b"SP");
        msg.extend_from_slice(pos.as_slice());
        msg.push(13);
        self.write(msg.as_slice()).await;
    }

    pub async fn set_velocity(&self, mut velocity: f64) {
        if velocity < 0. {
            velocity = 0.;
        }
        log::debug!("Called set vel with a parameter of {}, on motor id {}", velocity, self.id);
        let vel = num_to_bytes((velocity * (self.scale as f64)).trunc() as isize);
        let mut msg: Vec<u8> = Vec::with_capacity(vel.len() + self.prefix.len() + 1);
        msg.extend_from_slice(self.prefix.as_slice());
        msg.extend_from_slice(b"SV");
        msg.extend_from_slice(vel.as_slice());
        msg.push(13);
        self.write(msg.as_slice()).await;
    }

    pub async fn set_acceleration(&self, acceleration: f64) {
        log::debug!("Called set accel with a parameter of {}, on motor id {}", acceleration, self.id);
        let accel = num_to_bytes((acceleration * (self.scale as f64)).trunc() as isize);
        let mut msg: Vec<u8> = Vec::with_capacity(accel.len() + self.prefix.len() + 1);
        msg.extend_from_slice(self.prefix.as_slice());
        msg.extend_from_slice(b"SA");
        msg.extend_from_slice(accel.as_slice());
        msg.push(13);
        self.write(msg.as_slice()).await;
    }

    pub async fn set_deceleration(&self, deceleration: f64) {
        log::debug!("Called set del with a parameter of {}, on motor id {}", deceleration, self.id);
        let accel = num_to_bytes((deceleration * (self.scale as f64)).trunc() as isize);
        let mut msg: Vec<u8> = Vec::with_capacity(accel.len() + self.prefix.len() + 1);
        msg.extend_from_slice(self.prefix.as_slice());
        msg.extend_from_slice(b"SD");
        msg.extend_from_slice(accel.as_slice());
        msg.push(13);
        self.write(msg.as_slice()).await;
    }

    pub async fn get_status(&self) -> Status {
        let status_cmd = [2, b'M', self.id + 48, b'G', b'S', 13];
        let res = self.write(status_cmd.as_slice()).await;
        match res[3] {
            48 => Status::Disabled,
            49 => Status::Enabling,
            50 => Status::Faulted,
            51 => Status::Ready,
            52 => Status::Moving,
            _ => Status::Unknown,
        }
    }

    pub async fn get_position(&self) -> f64 {
        let get_pos_cmd = [2, b'M', self.id + 48, b'G', b'P', 13];
        let res = self.write(get_pos_cmd.as_slice()).await;
        (ascii_to_int(res.as_slice()) as f64) / (self.scale as f64)
    }

    pub async fn clear_alerts(&self) {
        let clear_cmd = [2, b'M', self.id + 48, b'C', b'A', 13];
        self.write(clear_cmd.as_slice()).await;
    }

    pub async fn wait_for_move(&self, interval: Duration) -> Result<(), Status> {
        let mut tick_interval = tokio::time::interval(interval);
        tick_interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            let status = self.get_status().await;
            match status {
                Status::Moving => {
                    tick_interval.tick().await;
                    continue;
                }
                Status::Disabled | Status::Faulted | Status::Unknown => return Err(status),
                Status::Ready => break,
                Status::Enabling => {
                    tick_interval.tick().await;
                    continue;
                }
            }
        }
        Ok(())
    }
}

impl SendRecv for ClearCoreMotor {
    fn get_sender(&self) -> &Sender<Message> {
        &self.drive_sender
    }
}

//
// #[tokio::test]
// pub async fn test_motor_enable_disable() {
//     //NOTE: It is UNSAFE to test motion unless we are right in front of Ryo therefore we're only
//     //Testing enable/disable and status in this automated test. For motion, we should test manually
//     let (m1tx, rx) = mpsc::channel::<Message>(100);
//     let m2tx = m1tx.clone();
//     let m3tx = m1tx.clone();
//     let m4tx = m1tx.clone();
//
//     let client = tokio::spawn(client("192.168.1.11:8888", rx));
//
//     let enable = tokio::spawn(async move {
//         let motor1 = AsyncMotor::new(0,800, Controller::new(m1tx));
//         let motor2 = AsyncMotor::new(1,800, Controller::new(m2tx));
//         let motor3 = AsyncMotor::new(2, 800, Controller::new(m3tx));
//         let motor4 = AsyncMotor::new(2, 800, Controller::new(m4tx));
//         motor1.enable().await.expect("No msg received...");
//         motor2.enable().await.expect("No msg received...");
//         motor3.enable().await.expect("No msg received...");
//         motor4.enable().await.expect("No msg received...");
//
//         //Give clear core and ethernet time to enable
//         tokio::time::sleep(Duration::from_millis(1000)).await;
//         //If a motor drive is not connected then Status will return faulted unless HLFB is disabled
//         //on ClearCore
//         let m1_status = motor1.get_status().await.expect("No msg received...");
//         assert_eq!(m1_status, Status::Ready);
//         let m2_status = motor2.get_status().await.expect("No msg received...");
//         assert_eq!(m2_status, Status::Ready);
//         let m3_status = motor3.get_status().await.expect("No msg received...");
//         assert_eq!(m3_status, Status::Ready);
//         let m4_status = motor4.get_status().await.expect("No msg received...");
//         assert_eq!(m4_status, Status::Ready);
//
//         motor1.disable().await.expect("No msg received...");
//         motor2.disable().await.expect("No msg received...");
//         motor3.disable().await.expect("No msg received...");
//         motor4.disable().await.expect("No msg received...");
//
//         let m1_status = motor1.get_status().await.expect("No msg received...");
//         assert_eq!(m1_status, Status::Disabled);
//         let m2_status = motor2.get_status().await.expect("No msg received...");
//         assert_eq!(m2_status, Status::Disabled);
//         let m3_status = motor3.get_status().await.expect("No msg received...");
//         assert_eq!(m3_status, Status::Disabled);
//         let m4_status = motor4.get_status().await.expect("No msg received...");
//         assert_eq!(m4_status, Status::Disabled);
//
//     });
//     client.await.unwrap().expect("TODO: panic message");
//     enable.await.unwrap();
// }
//
// #[tokio::test]
// pub async fn laptop_motor_enable_disable() {
//     //NOTE: It is UNSAFE to test motion unless we are right in front of Ryo therefore we're only
//     //Testing enable/disable and status in this automated test. For motion, we should test manually
//     let (m1tx, rx) = mpsc::channel::<Message>(100);
//     let m2tx = m1tx.clone();
//     let m3tx = m1tx.clone();
//     let m4tx = m1tx.clone();
//
//     let client = tokio::spawn(client("192.168.1.12:8888", rx));
//
//     let enable = tokio::spawn(async move {
//         let motor1 = AsyncMotor::new(0,800, Controller::new(m1tx));
//         let motor2 = AsyncMotor::new(1,800, Controller::new(m2tx));
//         let motor3 = AsyncMotor::new(2, 800, Controller::new(m3tx));
//         let motor4 = AsyncMotor::new(2, 800, Controller::new(m4tx));
//         motor1.enable().await.expect("No msg received...");
//         motor2.enable().await.expect("No msg received...");
//         motor3.enable().await.expect("No msg received...");
//         motor4.enable().await.expect("No msg received...");
//
//         //Give clear core and ethernet time to enable
//         tokio::time::sleep(Duration::from_millis(1000)).await;
//         //If a motor drive is not connected then Status will return faulted unless HLFB is disabled
//         //on ClearCore
//         let m1_status = motor1.get_status().await.expect("No msg received...");
//         assert_eq!(m1_status, Status::Ready);
//         let m2_status = motor2.get_status().await.expect("No msg received...");
//         assert_eq!(m2_status, Status::Ready);
//         let m3_status = motor3.get_status().await.expect("No msg received...");
//         assert_eq!(m3_status, Status::Ready);
//         let m4_status = motor4.get_status().await.expect("No msg received...");
//         assert_eq!(m4_status, Status::Ready);
//
//         motor1.disable().await.expect("No msg received...");
//         motor2.disable().await.expect("No msg received...");
//         motor3.disable().await.expect("No msg received...");
//         motor4.disable().await.expect("No msg received...");
//
//         let m1_status = motor1.get_status().await.expect("No msg received...");
//         assert_eq!(m1_status, Status::Disabled);
//         let m2_status = motor2.get_status().await.expect("No msg received...");
//         assert_eq!(m2_status, Status::Disabled);
//         let m3_status = motor3.get_status().await.expect("No msg received...");
//         assert_eq!(m3_status, Status::Disabled);
//         let m4_status = motor4.get_status().await.expect("No msg received...");
//         assert_eq!(m4_status, Status::Disabled);
//
//     });
//     client.await.unwrap().expect("TODO: panic message");
//     enable.await.unwrap();
// }

// #[tokio::test]
// async fn test_gantry() {
//     let (tx, rx) = tokio::sync::mpsc::channel(10);
//     let cc1_handler = tokio::spawn(client("192.168.1.11:8888", rx));
//     let motor = ClearCoreMotor::new(0, 800, tx);
//     let task = tokio::spawn(async move {
//         //motor.enable().await.unwrap();
//         let motor_status = motor.get_status().await.unwrap();
//         assert_eq!(motor_status, Status::Ready);
//         //motor.set_velocity(50.).await.unwrap();
//         motor.relative_move(-22.5).await.unwrap();
//     });
//     let (_, _) = tokio::join!(task, cc1_handler);
// }
//
// #[tokio::test]
// async fn test_gantry_pos() {
//     let (tx, rx) = tokio::sync::mpsc::channel(10);
//     let cc1_handler = tokio::spawn(client("192.168.1.11:8888", rx));
//     let motor = ClearCoreMotor::new(0, 800, tx);
//     let task = tokio::spawn(async move {
//         let motor_status = motor.get_status().await.unwrap();
//         assert_eq!(motor_status, Status::Ready);
//         //motor.set_velocity(50.).await.unwrap();
//         //motor.relative_move(-1.0).await.unwrap();
//         let pos = motor.get_position().await.unwrap();
//         println!("{pos}");
//     });
//     let (_, _) = tokio::join!(task, cc1_handler);
// }

use crate::components::clear_core_io::{DigitalInput, HBridgeState, Output, OutputState};
use crate::components::clear_core_motor::{ClearCoreMotor, Status};
use crate::interface::tcp::client;
use crate::subsystems::linear_actuator::{LinearActuator, SimpleLinearActuator};
use std::error::Error;
use std::time::Duration;
use tokio::time::sleep;
use crate::subsystems::gantry::GantryCommand;
use crate::subsystems::gantry::GantryCommand::GoTo;

pub struct BagGripper {
    motor: ClearCoreMotor,
    actuator: SimpleLinearActuator,
    positions: Vec<f64>, //Revs, we have to make a units crate for this
}

impl BagGripper {
    pub fn new(motor: ClearCoreMotor, actuator: SimpleLinearActuator, positions: Vec<f64>) -> Self {
        Self {
            motor,
            actuator,
            positions,
        }
    }

    pub async fn open(&self) -> Result<(), Box<dyn Error>> {
        self.actuator.actuate(HBridgeState::Pos).await.unwrap();
        sleep(Duration::from_secs_f64(2.0)).await;
        Ok(())
    }

    pub async fn close(&self) -> Result<(), Box<dyn Error>> {
        self.actuator.actuate(HBridgeState::Neg).await.unwrap();
        sleep(Duration::from_secs_f64(2.0)).await;
        Ok(())
    }
    pub async fn rip_bag(&self) -> Result<(), Box<dyn Error>> {
        for pos in self.positions.as_slice() {
            self.motor.relative_move(*pos).await.unwrap();
            self.motor
                .wait_for_move(Duration::from_millis(150))
                .await
                .unwrap();
        }
        Ok(())
    }
}

pub struct BagDispenser {
    motor: ClearCoreMotor,
    photo_eye: DigitalInput,
}

impl BagDispenser {
    pub fn new(motor: ClearCoreMotor, photo_eye: DigitalInput) -> Self {
        Self { motor, photo_eye }
    }
    pub async fn dispense(&self) -> Result<(), Box<dyn Error>> {
        self.motor.set_velocity(2.0).await.unwrap();
        self.motor.relative_move(400.0).await.unwrap();
        while !self.photo_eye.get_state().await.unwrap() {
            sleep(Duration::from_millis(100)).await;
        }
        self.motor.abrupt_stop().await.unwrap();
        Ok(())
    }
    pub async fn pull_back(&self) -> Result<(), Box<dyn Error>> {
        self.motor.set_velocity(1f64).await.unwrap();
        self.motor.relative_move(-4.5).await.unwrap();
        while self.motor.get_status().await.unwrap() == Status::Moving {
            sleep(Duration::from_millis(10)).await;
        }
        Ok(())
    }
}

pub async fn load_bag(bag_dispenser: BagDispenser, bag_gripper: BagGripper, blower: Output) {
    bag_gripper.close().await.unwrap();
    bag_dispenser.dispense().await.unwrap();
    blower.set_state(OutputState::On).await.unwrap();
    bag_gripper.open().await.unwrap();
    tokio::time::sleep(Duration::from_millis(1000)).await;
    bag_dispenser.pull_back().await.unwrap();
    bag_gripper.close().await.unwrap();
    blower.set_state(OutputState::Off).await.unwrap();
    bag_gripper.rip_bag().await.unwrap();
}

#[tokio::test]
async fn test_bag_dispense() {
    let (tx, rx) = tokio::sync::mpsc::channel(10);
    let cc1_handler = tokio::spawn(client("192.168.1.11:8888", rx));

    let bag_dispense_handler = tokio::spawn(async move {
        let motor = ClearCoreMotor::new(1, 200, tx.clone());
        motor.enable().await.unwrap();
        let state = motor.get_status().await.unwrap();
        assert_eq!(state, Status::Ready);
        let photo_eye = DigitalInput::new(1, tx);
        let dispenser = BagDispenser::new(motor, photo_eye);
        dispenser.dispense().await.unwrap();
        dispenser.pull_back().await.unwrap();
    });

    let (_, _) = tokio::join!(bag_dispense_handler, cc1_handler);
}

#[tokio::test]
async fn test_gripper_motor() {
    let (tx, rx) = tokio::sync::mpsc::channel(10);
    let (tx2, rx2) = tokio::sync::mpsc::channel(10);

    let cc1_handler = tokio::spawn(client("192.168.1.11:8888", rx));
    let cc2_handler = tokio::spawn(client("192.168.1.12:8888", rx2));

    let motor_handler = tokio::spawn(async move {
        let motor = ClearCoreMotor::new(2, 200, tx);
        let gripper = BagGripper::new(
            motor,
            SimpleLinearActuator::new(tx2, 4, 0),
            [0.3, -0.6, 0.3].to_vec(),
        );
        gripper.rip_bag().await.unwrap();
    });
    let (_, _, _) = tokio::join!(motor_handler, cc1_handler, cc2_handler);
}

#[tokio::test]
async fn test_gripper_actuator() {
    let (tx, rx) = tokio::sync::mpsc::channel(10);
    let (tx2, rx2) = tokio::sync::mpsc::channel(10);

    let cc1_handler = tokio::spawn(client("192.168.1.11:8888", rx));
    let cc2_handler = tokio::spawn(client("192.168.1.12:8888", rx2));

    let actuator_handler = tokio::spawn(async move {
        let motor = ClearCoreMotor::new(2, 200, tx);
        let gripper = BagGripper::new(
            motor,
            SimpleLinearActuator::new(tx2.clone(), 4, 0),
            [0.3, -0.6, 0.3].to_vec(),
        );
        gripper.open().await.unwrap();
        tokio::time::sleep(Duration::from_millis(2000)).await;
        gripper.close().await.unwrap();
    });
    let (_, _, _) = tokio::join!(actuator_handler, cc1_handler, cc2_handler);
}

#[tokio::test]
async fn test_bag_loading() {
    let (tx, rx) = tokio::sync::mpsc::channel(10);
    let (tx2, rx2) = tokio::sync::mpsc::channel(10);

    let cc1_handler = tokio::spawn(client("192.168.1.11:8888", rx));
    let cc2_handler = tokio::spawn(client("192.168.1.12:8888", rx2));
    let task = tokio::spawn(async move {
        let disp_motor = ClearCoreMotor::new(1, 200, tx.clone());
        let grip_motor = ClearCoreMotor::new(2, 200, tx.clone());
        disp_motor.enable().await.unwrap();
        grip_motor.enable().await.unwrap();
        tokio::time::sleep(Duration::from_millis(500)).await;
        let dispenser = BagDispenser::new(disp_motor, DigitalInput::new(1, tx.clone()));
        let gripper = BagGripper::new(
            grip_motor,
            SimpleLinearActuator::new(tx2.clone(), 4, 0),
            [0.4, -0.8, 0.4].to_vec(),
        );
        let blower = Output::new(5, tx2);
        load_bag(dispenser, gripper, blower, /* tokio::sync::mpsc::Sender<GantryCommand> */).await;
        let gantry = ClearCoreMotor::new(0, 800, tx);

        tokio::time::sleep(Duration::from_millis(100)).await;
        gantry.relative_move(25.0).await.unwrap();
    });
    let (_, _, _) = tokio::join!(task, cc1_handler, cc2_handler);
}

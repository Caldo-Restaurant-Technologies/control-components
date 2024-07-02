use crate::components::clear_core_motor::{ClearCoreMotor, Status};
use crate::interface::tcp::client;
use std::error::Error;
use std::time::Duration;
use tokio::sync::mpsc::Receiver;
use tokio::sync::oneshot;

pub enum GantryCommand {
    GetPosition(oneshot::Sender<f64>),
    GoTo(f64),
}

pub async fn gantry(
    motor: ClearCoreMotor,
    mut rx: Receiver<GantryCommand>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    motor.set_acceleration(40.).await.unwrap();
    motor.set_velocity(300.).await.unwrap();
    motor.enable().await.unwrap();
    while let Some(cmd) = rx.recv().await {
        match cmd {
            GantryCommand::GetPosition(sender) => {
                let pos = motor.get_position().await.unwrap();
                sender.send(pos).unwrap();
            }
            GantryCommand::GoTo(pos) => {
                motor.absolute_move(pos).await.unwrap();
                while motor.get_status().await.unwrap() == Status::Moving {
                    tokio::time::sleep(Duration::from_secs_f64(1.0)).await;
                }
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn test_gantry() {
    let positions = vec![92.0, 24.5, 47.0, 69.5, 92.0];
    let (tx, rx) = tokio::sync::mpsc::channel(10);
    let (gtx, grx) = tokio::sync::mpsc::channel(10);
    let gantry_handler = tokio::spawn(gantry(ClearCoreMotor::new(0, 800, tx), grx));
    let cc1_handler = tokio::spawn(client("192.168.1.11:8888", rx));

    let goto = tokio::spawn(async move {
        for pos in positions {
            gtx.send(GantryCommand::GoTo(pos)).await.unwrap();
            let (rep_tx, rep_rx) = oneshot::channel();
            let msg = GantryCommand::GetPosition(rep_tx);
            gtx.send(msg).await.unwrap();
            let rep = rep_rx.await.unwrap();
            println!("in position: {rep}");
        }
    });

    let (_, _, _) = tokio::join!(goto, gantry_handler, cc1_handler);
}

#[tokio::test]
async fn test_gantry_home() {
    let pos = -0.25;
    let (tx, rx) = tokio::sync::mpsc::channel(10);
    let (gtx, grx) = tokio::sync::mpsc::channel(10);
    let gantry_handler = tokio::spawn(gantry(ClearCoreMotor::new(0, 800, tx), grx));
    let cc1_handler = tokio::spawn(client("192.168.1.11:8888", rx));

    let goto = tokio::spawn(async move {
        //gtx.send(GantryCommand::GoTo(pos)).await.unwrap();
        let (rep_tx, rep_rx) = oneshot::channel();
        let msg = GantryCommand::GetPosition(rep_tx);
        gtx.send(msg).await.unwrap();
        let rep = rep_rx.await.unwrap();
        println!("in position: {rep}");
    });

    let (_, _, _) = tokio::join!(goto, gantry_handler, cc1_handler);
}

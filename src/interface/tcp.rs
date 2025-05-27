use crate::controllers::clear_core::Message;
use log::error;
use std::error::Error;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpStream, ToSocketAddrs};
use tokio::sync::mpsc;
use tokio::time::MissedTickBehavior;

pub async fn client<T: ToSocketAddrs>(
    addr: T,
    mut msg: mpsc::Receiver<Message>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let mut stream = TcpStream::connect(addr).await?;
    if let Err(e) = stream.peer_addr() {
        error!("{}",e);
    };
    let mut tick_interval = tokio::time::interval(Duration::from_millis(5));
    tick_interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
    while let Some(message) = msg.recv().await {
        stream.write_all(&message.buffer).await?;
        stream.readable().await?;
        let mut buffer = [0; 100];
        match stream.read(&mut buffer).await {
            Ok(0) => {
                error!("Connection closed by server");
            }
            Ok(_) => {
                if message.response.send(buffer.to_vec()).is_err() {
                    error!("Failed to send via channel");
                }
            }
            Err(e) => {
                error!("Failed to read from stream: {}", e);
                break;
            }
        }
        tick_interval.tick().await;
    }
    Ok(())
}

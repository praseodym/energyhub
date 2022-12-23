use std::error::Error;
use std::str;
use std::time::Duration;

use rumqttc::{AsyncClient, Event, Incoming, MqttOptions, Packet, QoS, SubscribeFilter};
use rusqlite::{Connection, Result};

use energyhub::{connect_sqlite, DSMRMeasurements, KamstrupValues};

const DSMR_TOPIC: &'static str = "dsmr/measurements";
const KAMSTRUP_TOPIC: &'static str = "kamstrup/values";

#[tokio::main]
async fn main() -> Result<()> {
    let conn = connect_sqlite()?;

    let mut mqttoptions = MqttOptions::new("mqtt2sqlite", "127.0.0.1", 1883);
    mqttoptions.set_keep_alive(Duration::from_secs(5));

    let (client, mut eventloop) = AsyncClient::new(mqttoptions, 10);

    loop {
        while let Ok(notification) = eventloop.poll().await {
            match notification {
                Event::Incoming(Incoming::Publish(p)) => match p.topic.as_str() {
                    DSMR_TOPIC => {
                        let measurements: DSMRMeasurements =
                            serde_json::from_slice(&p.payload).unwrap();
                        println!("deserialized dsmr = {:?}", measurements);
                        let timestamp = &measurements.timestamp.timestamp();
                        let active_tariff = match measurements.active_tariff.as_str() {
                            "Tariff1" => 1,
                            "Tariff2" => 2,
                            _ => 0,
                        };
                        let used_t1 = (measurements.electricity_used_t1 * 1e3f64) as u32;
                        let used_t2 = (measurements.electricity_used_t2 * 1e3f64) as u32;
                        let current_usage =
                            (measurements.current_electricity_usage * 1e3f64) as u32;
                        conn.execute("INSERT INTO electricity (timestamp, used_t1, used_t2, active_tariff, current_usage) VALUES (?, ?, ?, ?, ?) ON CONFLICT DO NOTHING",       
                                         (&timestamp, &used_t1, &used_t2, &active_tariff, &current_usage),                            )?;
                    }
                    KAMSTRUP_TOPIC => {
                        let result = kamstrup(&conn, &p.payload);
                        match result {
                            Ok(_) => {}
                            Err(e) => {
                                println!("kamstrup: error parsing payload: {:?}", &p.payload);
                                println!("{}", e);
                            }
                        }
                    }
                    _ => println!("received message from unknown topic \"{}\"", p.topic),
                },
                Event::Incoming(Packet::ConnAck(ack)) => {
                    if !ack.session_present {
                        subscribe(&client).await;
                    }
                }
                Event::Incoming(i) => {
                    println!("debug: incoming = {:?}", i);
                }
                Event::Outgoing(o) => println!("debug: outgoing = {:?}", o),
                // Err(e) => {
                //     println!("Error = {:?}", e);
                // }
            }
        }
        // TODO: handle err
    }
    // Ok(())
}

fn kamstrup(conn: &Connection, payload: &[u8]) -> Result<(), Box<dyn Error>> {
    let values: KamstrupValues = serde_json::from_slice(payload)?;
    println!("deserialized kamstrup = {:?}", values);
    let timestamp = &values.timestamp.timestamp();
    let energy = (values.energy * 1e3f64) as u32;
    let volume = (values.volume * 1e3f64) as u32;
    let hourcounter = values.hourcounter as u32;
    // let used_t2 = (measurements.electricity_used_t2 * 1e3f64) as u32;
    // let current_usage =
    //     (measurements.current_electricity_usage * 1e3f64) as u32;
    conn.execute(
        "INSERT INTO heat (timestamp, energy, volume, hourcounter) VALUES (?, ?, ?, ?) ON CONFLICT DO NOTHING",
        (&timestamp, &energy, &volume, &hourcounter),
    )?;
    Ok(())
}

async fn subscribe(client: &AsyncClient) {
    client
        .subscribe_many(vec![
            SubscribeFilter::new(DSMR_TOPIC.to_string(), QoS::AtLeastOnce),
            SubscribeFilter::new(KAMSTRUP_TOPIC.to_string(), QoS::AtLeastOnce),
        ])
        .await
        .unwrap();
    println!("subscribed");
}

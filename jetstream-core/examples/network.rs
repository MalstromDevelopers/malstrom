use std::{
    collections::{HashMap, VecDeque}, net::{SocketAddr, SocketAddrV4}, path::Path, rc::Rc, sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    }, time::{Duration, Instant}
};

use apache_avro::{from_value, AvroSchema, Reader, Schema};
use glob::glob;
use itertools::Itertools;
use jetstream::time;
use jetstream::{
    channels::selective_broadcast::{Receiver, Sender},
    config::Config,
    filter::Filter,
    keyed::{KeyDistribute, KeyLocal},
    snapshot::NoopPersistenceBackend,
    stateful_map::StatefulMap,
    stream::operator::OperatorBuilder,
    time::NoTime,
    worker::{self, Worker},
    DataMessage, Message, NoData, NoKey,
};
use serde::{Deserialize, Serialize};
use tonic::transport::Uri;
use url::Url;

static ALLOWED_OUT_OF_BOUNDNESS: u64 = 20 * 1000; // seconds
static STATE_SIZE: usize = 10;
static PREDICTION_NUM: usize = 5;

/// Code I stole from Nico
fn main() {
    let config = Config {
        worker_id: 0,
        port: 29090,
        cluster_addresses: Some(vec![
            "http://localhost:29091".into(),
            "http://localhost:29092".into(),
            "http://localhost:29093".into(),
            "http://localhost:29094".into(),
        ]),
        initial_scale: 2,
        sts_name: None,
    };
    let base_port: u16 = 29091;
    let range = 0..10u16;
    let addresses = range
        .clone()
        .map(|x| x + base_port)
        .map(|x| format!("http://localhost:{x}"))
        .collect_vec();
    let configs = range.map(|x| Config {
        worker_id: x.try_into().unwrap(),
        port: base_port + x,
        cluster_addresses: Some(addresses.clone()),
        initial_scale: addresses.len(),
        sts_name: None,
    });
    let start = Instant::now();
    let threads = configs
        .map(|c| std::thread::spawn(move || run_stream_a(c)))
        .collect_vec();

    for x in threads {
        let _ = x.join();
    }

}

fn run_stream_a(config: Config) {
    let mut globber = glob("/Users/damionwerner/Desktop/benchmark-data/*.avro").unwrap();
    let mut worker = Worker::<NoopPersistenceBackend>::new(|| false);

    let eof = Rc::new(AtomicBool::new(false));
    let eof_moved = eof.clone();
    let start = Instant::now();
    
    let stream = worker
        .new_stream()
        .then(OperatorBuilder::direct(move |input, output, ctx| {
            if ctx.worker_id != 0 {
                return;
            }

            if let Some(x) = globber.next() {
                let x = x.unwrap();
                let t = Instant::now();
                println!("{x:?} @ {t:?}");
                let content = read_avro(&x);
                for c in content {
                    output.send(jetstream::Message::Data(DataMessage::new(NoKey, c, 0)));
                }
                let t = Instant::now();
                println!("{t:?}");
            } else {
                eof_moved.store(true, Ordering::Relaxed);
                let elapsed = Instant::now().duration_since(start);
                println!("That took {elapsed:?}");
            }
        }))
        .key_distribute(
            |msg| calculate_hash(&msg.value.icao24),
            |key, targets| {
                let len = targets.len() - 1;
                let key: usize = (*key).try_into().unwrap();
                targets.get_index((key % len) + 1).unwrap()
            },
        )
        .filter(|value| {
            value.longitude.is_some() && value.time_position.is_some() && value.velocity.is_some()
        })
        // acumulate state
        .stateful_map(|val, state: &mut VecDeque<StateVector>| {
            state.push_back(val);
            if state.len() > STATE_SIZE {
                // remove the oldest position
                state.pop_front();
            }
            state.clone()
        })
        .filter(|value| value.len() > 2)
        .stateful_map(|mut trajectory, state: &mut ()| {
            // println!("Predicting...");
            for _ in 0..PREDICTION_NUM {
                let new_sv = trajectory.predict();
                trajectory.push_back(new_sv);
            }
            trajectory
        })
        // destruct all the messages
        .filter(|_| false);

    worker.add_stream(stream);
    let mut worker = worker.build(config).unwrap();

    while !eof.load(Ordering::Relaxed) {
        worker.step()
    }
}

#[derive(Debug, AvroSchema, Serialize, Deserialize, Clone)]
struct StateVector {
    // ICAO24 address of the transmitter in hex string representation.
    icao24: String,
    // callsign of the vehicle. Can be None if no callsign has been received.
    callsign: Option<String>,
    // inferred through the ICAO24 address.
    origin_country: Option<String>,
    // seconds since epoch of last position report. Can be None if there was no position report received by OpenSky within 15s before.
    time_position: Option<i64>,
    // seconds since epoch of last received message from this transponder.
    last_contact: i64,
    // in ellipsoidal coordinates (WGS-84) and degrees. Can be None.
    longitude: Option<f64>,
    // in ellipsoidal coordinates (WGS-84) and degrees. Can be None.
    latitude: Option<f64>,
    // geometric altitude in meters. Can be None.
    geo_altitude: Option<f64>,
    // true if aircraft is on ground (sends ADS-B surface position reports).
    on_ground: bool,
    // over ground in m/s. Can be None if information not present.
    velocity: Option<f64>,
    // in decimal degrees (0 is north). Can be None if information not present.
    true_track: Option<f64>,
    // in m/s, incline is positive, decline negative. Can be None if information not present.
    vertical_rate: Option<f64>,
    //  - serial numbers of sensors which received messages from the vehicle within the validity period of this state vector. Can be None if no filtering for sensor has been requested.
    sensors: Option<Vec<i64>>,
    // barometric altitude in meters. Can be None.
    baro_altitude: Option<f64>,
    // transponder code aka Squawk. Can be None.
    squawk: Option<String>,
    // special purpose indicator.
    spi: bool,
    // origin of this state’s position: 0 = ADS-B, 1 = ASTERIX, 2 = MLAT, 3 = FLARM, 4 = PREDICTION
    position_source: i8,
    // aircraft category: 0 = No information at all, 1 = No ADS-B Emitter Category Information, 2 = Light (< 15500 lbs), 3 = Small (15500 to 75000 lbs), 4 = Large (75000 to 300000 lbs), 5 = High Vortex Large (aircraft such as B-757), 6 = Heavy (> 300000 lbs), 7 = High Performance (> 5g acceleration and 400 kts), 8 = Rotorcraft, 9 = Glider / sailplane, 10 = Lighter-than-air, 11 = Parachutist / Skydiver, 12 = Ultralight / hang-glider / paraglider, 13 = Reserved, 14 = Unmanned Aerial Vehicle, 15 = Space / Trans-atmospheric vehicle, 16 = Surface Vehicle – Emergency Vehicle, 17 = Surface Vehicle – Service Vehicle, 18 = Point Obstacle (includes tethered balloons), 19 = Cluster Obstacle, 20 = Line Obstacle.
    // category: Category,
}
use std::fs::File;

fn read_avro(path: &Path) -> Vec<StateVector> {
    // create an Avro schema from the struct
    let schema = Schema::from(StateVector::get_schema());
    let mut total_produced_records = 0;

    // iterate over all avro files in the merged folder
    let file = File::options().read(true).open(&path).unwrap();
    let reader = Reader::with_schema(&schema, file).unwrap();

    let mut produced_records_successful = 0;

    // iterate over read records and write them to the Kafka endpoint
    reader
        .into_iter()
        .filter(|value| value.is_ok())
        .map(|value| from_value::<StateVector>(&value.unwrap()).unwrap())
        .collect()
}

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

fn calculate_hash<T: Hash>(t: &T) -> u64 {
    let mut s = DefaultHasher::new();
    t.hash(&mut s);
    s.finish()
}

use libm::{asin, atan2};
use std::f64::consts::PI;
use std::time::{SystemTime, UNIX_EPOCH};

/// Defines the Predictor trait
pub trait Predictor<T> {
    /// The function will return one new point based
    /// on itself.
    fn predict(&self) -> T;
}

impl Predictor<StateVector> for VecDeque<StateVector> {
    fn predict(&self) -> StateVector {
        // calculate the bearing and time delta between
        // every two consecutive positions
        let mut sliding_values: Vec<(Option<f64>, i64)> = vec![];
        for i in 1..self.len() {
            let sv1 = &self[i - 1];
            let sv2 = &self[i];
            sliding_values.push((
                calculate_bearing(sv1, sv2),
                sv2.time_position.unwrap() - sv1.time_position.unwrap(),
            ))
        }

        let avg_bearing = average_bearing(sliding_values.iter().filter_map(|x| x.0).collect());

        // radius of the earth
        let radius = 6371.0;

        let avg_delta_time =
            sliding_values.iter().map(|x| x.1).sum::<i64>() / (sliding_values.len() as i64);

        let avg_speed = self.iter().map(|x| x.velocity.unwrap()).sum::<f64>() / self.len() as f64;

        // convert speed to km/h
        let speed_kmph = avg_speed * 3.6;
        let avg_bearing_rad = avg_bearing.unwrap().to_radians();

        // calculate the distance traveled given speed and time
        let distance = speed_kmph * (avg_delta_time as f64 / 3600.0);

        let last: &StateVector = &self[self.len() - 1];

        // get the last points lat lon variables
        let lat1: f64 = last.latitude.unwrap().to_radians();
        let lon1 = last.longitude.unwrap().to_radians();

        // calculate the predicted lat
        let lat2 = asin(
            lat1.sin() * (distance / radius).cos()
                + lat1.cos() * (distance / radius).sin() * avg_bearing_rad.cos(),
        );
        // calculate the predicted lon
        let lon2 = lon1
            + atan2(
                avg_bearing_rad.sin() * (distance / radius).sin() * (lat1).cos(),
                (distance / radius).cos() - lat1.sin() * lat2.sin(),
            );

        // convert from radians to degree
        let new_lat = lat2.to_degrees();
        let new_lon = lon2.to_degrees();

        // create new StateVector from previous message and newly calculated fields
        let mut new_sv = last.clone();
        new_sv.latitude = Some(new_lat);
        new_sv.longitude = Some(new_lon);
        new_sv.time_position = Some(new_sv.time_position.unwrap() + avg_delta_time);
        new_sv.last_contact = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_millis() as i64;
        new_sv.position_source = 4;

        new_sv
    }
}

/// The function calculate the bearing between two geographic positions.
///
/// It returns the bearing to true noth in the range between 0 and 360
///
/// # Example
/// ```rust
/// let sv1 = StateVecotr{...};
/// let sv2 = StateVecotr{...};
/// let bearing =  geographic_helper::calculate_bearing(sv1, sv2);
/// ```
pub fn calculate_bearing(sv1: &StateVector, sv2: &StateVector) -> Option<f64> {
    if sv1.longitude.is_none()
        || sv1.latitude.is_none()
        || sv2.longitude.is_none()
        || sv2.latitude.is_none()
    {
        return None;
    }

    let d_lon: f64 = sv2.longitude.unwrap() - sv1.longitude.unwrap();
    let y = d_lon.sin() * sv2.latitude.unwrap().cos();
    let x = sv1.latitude.unwrap().cos() * sv2.latitude.unwrap().sin()
        - sv1.latitude.unwrap().sin() * sv2.latitude.unwrap().cos() * d_lon.cos();
    let brng = y.atan2(x).to_degrees();

    Some(360.0 - ((brng + 360.0) % 360.0))
}

/// Calculate the average bearing from a list of bearings
///
/// # Example
/// ```rust
/// let avg_bearing = geographic_helper::average_bearing(vec![1,2,3,4]);
/// ```
pub fn average_bearing(angles: Vec<f64>) -> Result<f64, &'static str> {
    if angles.is_empty() {
        return Err("Require at least one element");
    }

    let len = angles.len() as f64;
    let avg_x = angles
        .iter()
        .map(|angle| (angle * PI / 180.0).cos())
        .sum::<f64>()
        / len;
    let avg_y = angles
        .iter()
        .map(|angle| (angle * PI / 180.0).sin())
        .sum::<f64>()
        / len;

    let avg_angle = libm::atan2(avg_y, avg_x) * 180.0 / PI;

    let normalized_angle = if avg_angle < 0.0 {
        avg_angle + 360.0
    } else {
        avg_angle
    };

    Ok(normalized_angle % 360.0)
}

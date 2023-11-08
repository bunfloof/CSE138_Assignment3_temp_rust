use actix_web::{delete, get, put, web, App, HttpResponse, HttpServer, Responder, HttpRequest};
use log::{info, error};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::HashMap;
use std::env;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::time::interval;

#[derive(Serialize, Deserialize)]
struct KeyValue {
    value: serde_json::Value,
    #[serde(rename = "causal-metadata")]
    causal_metadata: Option<HashMap<String, u64>>,
}

#[derive(Deserialize)]
struct GetRequest {
    #[serde(rename = "causal-metadata")]
    causal_metadata: HashMap<String, u64>,
}

#[derive(Deserialize)]
struct DeleteRequest {
    #[serde(rename = "causal-metadata")]
    causal_metadata: Option<HashMap<String, u64>>,
}

#[derive(Serialize, Deserialize)]
struct Heartbeat {
    socket_address: String,
}

struct AppState {
    store: Mutex<HashMap<String, serde_json::Value>>,
    vector_clock: Arc<Mutex<HashMap<String, u64>>>,
    replicas: Arc<Mutex<Vec<String>>>, // Now wrapped in Arc
    socket_address: String,
    last_heartbeat_received: Mutex<HashMap<String, Instant>>, // Tracking last heartbeat times
}

// Utility function to update the vector clock
fn update_vector_clock(clock: Arc<Mutex<HashMap<String, u64>>>, replica_id: &str) {
    let mut clock = clock.lock().unwrap();
    let count = clock.entry(replica_id.to_string()).or_insert(0);
    *count += 1;
}

// Utility function to merge vector clocks
fn merge_vector_clocks(local_clock: &mut HashMap<String, u64>, received_clock: &HashMap<String, u64>) {
    for (replica, count) in received_clock {
        let local_count = local_clock.entry(replica.clone()).or_default();
        *local_count = std::cmp::max(*local_count, *count);
    }
}

// Utility function to check if causally ready
fn is_causally_ready(local_clock: &HashMap<String, u64>, client_metadata: &HashMap<String, u64>) -> bool {
    for (replica, client_count) in client_metadata {
        let local_count = local_clock.get(replica).unwrap_or(&0);
        info!("Checking causal readiness for replica: {}. Local count: {}, Client count: {}", replica, local_count, client_count);
        
        if local_count < client_count {
            info!("Not causally ready. Replica {} is behind. Local count: {}, Client count: {}", replica, local_count, client_count);
            return false;
        }
    }
    
    info!("Causally ready.");
    true
}

async fn send_heartbeats(state: web::Data<AppState>) {
    let client = Client::new();
    let mut interval = interval(Duration::from_secs(1));
    loop {
        interval.tick().await;
        let replicas = state.replicas.lock().unwrap().clone();
        for replica in &replicas {
            if replica != &state.socket_address {
                let replica_clone = replica.clone();
                let state_clone = state.clone();
                let client_clone = client.clone(); // Clone the client for each iteration
                tokio::spawn(async move {
                    let start = Instant::now();
                    let result = client_clone.put(format!("http://{}/heartbeat", &replica_clone))
                        .json(&Heartbeat {
                            socket_address: state_clone.socket_address.clone(),
                        })
                        .timeout(Duration::from_millis(500)) // set a timeout of 500ms for the request
                        .send()
                        .await;
                    let elapsed = start.elapsed();
                    match result {
                        Ok(_) => info!("Heartbeat sent to {} successfully in {:?}", replica_clone, elapsed),
                        Err(e) => error!("Error sending heartbeat to {}: {} (took {:?})", replica_clone, e, elapsed),
                    }
                });
            }
        }
    }
}

async fn check_for_failed_replicas(state: web::Data<AppState>) {
    let mut interval = interval(Duration::from_secs(1));
    loop {
        interval.tick().await;
        let now = Instant::now();
        let mut last_heartbeat_received = state.last_heartbeat_received.lock().unwrap();
        let mut replicas = state.replicas.lock().unwrap();

        // Filter out replicas that are considered down
        replicas.retain(|replica| {
            if replica == &state.socket_address {
                true // Always keep the current replica
            } else if let Some(last_heartbeat) = last_heartbeat_received.get(replica) {
                if now.duration_since(*last_heartbeat) <= Duration::from_secs(3) {
                    true // Keep the replica, as it's within the heartbeat timeout
                } else {
                    info!("Replica {} is down and removed from view.", replica);
                    false // Remove the replica
                }
            } else {
                // If no heartbeat received yet, keep the replica for now
                info!("No heartbeat received from {}, keeping in view for now.", replica);
                true
            }
        });
    }
}


#[put("/heartbeat")]
async fn receive_heartbeat(req: web::Json<Heartbeat>, state: web::Data<AppState>) -> impl Responder {
    let mut last_heartbeat_received = state.last_heartbeat_received.lock().unwrap();
    last_heartbeat_received.insert(req.socket_address.clone(), Instant::now());
    HttpResponse::Ok()
}

#[get("/view")]
async fn get_view(state: web::Data<AppState>) -> impl Responder {
    let replicas = state.replicas.lock().unwrap();
    HttpResponse::Ok().json(&*replicas)
}

#[put("/kvs/{key}")]
async fn put(key: web::Path<String>, item: web::Json<KeyValue>, data: web::Data<AppState>) -> impl Responder {
    let key_str = key.into_inner();
    info!("Received PUT request for key: {}", key_str);

    if key_str.len() > 50 {
        info!("Key length exceeds limit.");
        return HttpResponse::BadRequest().json(json!({ "error": "Key is too long" }));
    }

    let mut store_lock = data.store.lock().unwrap();
    
    {
        let vector_clock_lock = data.vector_clock.lock().unwrap();
        if !is_causally_ready(&vector_clock_lock, item.causal_metadata.as_ref().unwrap_or(&HashMap::new())) {
            info!("Not causally ready for key: {}", key_str);
            return HttpResponse::ServiceUnavailable().json(json!({ "error": "Causal dependencies not satisfied; try again later" }));
        }
    }

    update_vector_clock(data.vector_clock.clone(), &data.socket_address);

    let result = if store_lock.contains_key(&key_str) {
        info!("Replacing existing key: {}", key_str);
        store_lock.insert(key_str.clone(), item.value.clone());
        HttpResponse::Ok().json(json!({ "result": "replaced", "causal-metadata": *data.vector_clock.lock().unwrap() }))
    } else {
        info!("Creating new key: {}", key_str);
        store_lock.insert(key_str.clone(), item.value.clone());
        HttpResponse::Created().json(json!({ "result": "created", "causal-metadata": *data.vector_clock.lock().unwrap() }))
    };

    let broadcast_data = KeyValue { 
        value: item.value.clone(), 
        causal_metadata: Some(data.vector_clock.lock().unwrap().clone()) 
    };

    let broadcast_result = broadcast_write("put", &key_str, &broadcast_data, &data).await;
    if let Err(e) = broadcast_result {
        error!("Error during broadcast: {}", e);
    }

    result
}

#[get("/kvs/{key}")]
async fn get(key: web::Path<String>, data: web::Data<AppState>, req_body: web::Json<GetRequest>) -> impl Responder {
    let key_str = key.into_inner();
    let causal_metadata = req_body.into_inner().causal_metadata;

    let store = data.store.lock().unwrap();

    if !is_causally_ready(&data.vector_clock.lock().unwrap(), &causal_metadata) {
        return HttpResponse::ServiceUnavailable().json(json!({ "error": "Causal dependencies not satisfied; try again later" }));
    }

    if let Some(value) = store.get(&key_str) {
        HttpResponse::Ok().json(json!({ "result": "found", "value": value, "causal-metadata": *data.vector_clock.lock().unwrap() }))
    } else {
        HttpResponse::NotFound().json(json!({ "error": "Key does not exist", "causal-metadata": *data.vector_clock.lock().unwrap() }))
    }
}

#[delete("/kvs/{key}")]
async fn delete(key: web::Path<String>, req_body: web::Json<DeleteRequest>, data: web::Data<AppState>) -> impl Responder {
    let key_str = key.into_inner();
    let causal_metadata = req_body.into_inner().causal_metadata.unwrap_or_default();

    let mut store = data.store.lock().unwrap();

    if !is_causally_ready(&data.vector_clock.lock().unwrap(), &causal_metadata) {
        return HttpResponse::ServiceUnavailable().json(json!({ "error": "Causal dependencies not satisfied; try again later" }));
    }

    update_vector_clock(data.vector_clock.clone(), &data.socket_address);

    let deletion_successful = store.remove(&key_str).is_some();

    let broadcast_data = KeyValue { 
        value: serde_json::Value::Null, 
        causal_metadata: Some(data.vector_clock.lock().unwrap().clone()) 
    };

    let broadcast_result = broadcast_write("delete", &key_str, &broadcast_data, &data).await;
    if let Err(e) = broadcast_result {
        error!("Error during broadcast: {}", e);
    }

    if deletion_successful {
        HttpResponse::Ok().json(json!({ "result": "deleted", "causal-metadata": *data.vector_clock.lock().unwrap() }))
    } else {
        HttpResponse::NotFound().json(json!({ "error": "Key does not exist", "causal-metadata": *data.vector_clock.lock().unwrap() }))
    }
}

async fn broadcast_write(method: &str, key: &str, data: &KeyValue, app_state: &web::Data<AppState>) -> Result<(), reqwest::Error> {
    let client = Client::builder().timeout(Duration::from_secs(1)).build()?;
    let current_replica = &app_state.socket_address;

    let replicas = app_state.replicas.lock().unwrap(); // Locking the replicas

    for replica in replicas.iter() {
        if replica == current_replica {
            continue; // Skip broadcasting to itself
        }

        let url = format!("http://{}/internal/kvs/{}", replica, key);
        info!("Broadcasting {} to {}", method, url);

        let result = match method {
            "put" => client.put(&url).json(data).send().await,
            "delete" => client.delete(&url).send().await,
            _ => unreachable!(),
        };

        match result {
            Ok(_) => info!("Broadcast to {} successful.", url),
            Err(e) => {
                error!("Error broadcasting to {}: {}", url, e);
                return Err(e);
            }
        };
    }

    Ok(())
}

#[put("/internal/kvs/{key}")]
async fn internal_put(key: web::Path<String>, item: web::Json<KeyValue>, data: web::Data<AppState>) -> impl Responder {
    let key_str = key.into_inner();

    let mut store = data.store.lock().unwrap();
    let mut vector_clock = data.vector_clock.lock().unwrap();

    if let Some(received_clock) = &item.causal_metadata {
        merge_vector_clocks(&mut *vector_clock, received_clock);
    }

    store.insert(key_str.clone(), item.value.clone());

    HttpResponse::Ok().json(json!({ "result": "updated", "causal-metadata": *vector_clock }))
}

#[delete("/internal/kvs/{key}")]
async fn internal_delete(key: web::Path<String>, req_body: web::Json<DeleteRequest>, data: web::Data<AppState>) -> impl Responder {
    let key_str = key.into_inner();
    let causal_metadata = req_body.into_inner().causal_metadata.unwrap_or_default();

    let mut store = data.store.lock().unwrap();
    let mut vector_clock = data.vector_clock.lock().unwrap();

    merge_vector_clocks(&mut vector_clock, &causal_metadata);

    if store.remove(&key_str).is_some() {
        HttpResponse::Ok().json(json!({ "result": "deleted", "causal-metadata": *vector_clock }))
    } else {
        HttpResponse::NotFound().json(json!({ "error": "Key does not exist", "causal-metadata": *vector_clock }))
    }
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    env_logger::init();
    log::info!("Starting server...");

    let view = env::var("VIEW").unwrap_or_default();
    let replicas: Vec<String> = view.split(',').map(String::from).collect();
    let vector_clock = initialize_vector_clock(&replicas);

    let socket_address = env::var("SOCKET_ADDRESS").unwrap_or_else(|_| "localhost".to_string());
    let port = env::var("PORT").unwrap_or_else(|_| "8090".to_string());

    let app_data = web::Data::new(AppState {
        store: Mutex::new(HashMap::new()),
        vector_clock: Arc::new(Mutex::new(vector_clock)),
        replicas: Arc::new(Mutex::new(replicas)),
        socket_address,
        last_heartbeat_received: Mutex::new(HashMap::new()),
    });

    let heartbeat_handle = tokio::spawn(send_heartbeats(app_data.clone()));
    let check_replicas_handle = tokio::spawn(check_for_failed_replicas(app_data.clone()));

    HttpServer::new(move || {
        App::new()
            .app_data(app_data.clone())
            .service(put)
            .service(get)
            .service(get_view)
            .service(delete)
            .service(internal_put)
            .service(internal_delete)
            .service(receive_heartbeat)
    })
    .bind(format!("0.0.0.0:{}", port))?
    .run()
    .await
}

fn initialize_vector_clock(replicas: &[String]) -> HashMap<String, u64> {
    replicas.iter().map(|replica| (replica.clone(), 0)).collect()
}
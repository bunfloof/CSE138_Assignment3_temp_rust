use actix_web::{delete, get, put, web, App, HttpResponse, HttpServer, Responder};
use log::{info, error};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::HashMap;
use std::env;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::time::interval;
use tokio::sync::RwLock;

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

#[derive(Deserialize, Debug)]
struct HeartbeatRequest {
    socket_address: String,
}

#[derive(Deserialize)]
struct ViewChangeRequest {
    socket_address: String,
}

#[derive(Deserialize, Debug)]
struct InternalViewChangeRequest {
    socket_address: String,
    action: String,
}

struct AppState {
    store: Mutex<HashMap<String, serde_json::Value>>,
    vector_clock: Arc<Mutex<HashMap<String, u64>>>,
    socket_address: String,
    replicas: Vec<String>, // Static list of all possible replicas
    view: Arc<Mutex<Vec<String>>>, // Dynamic list of active replicas
    last_heartbeat_received: Arc<RwLock<HashMap<String, Instant>>>,
}

async fn send_heartbeats(app_state: web::Data<AppState>) {
    let mut interval = interval(Duration::from_millis(1000));
    let client = Client::new();

    loop {
        interval.tick().await;
        let replicas = app_state.replicas.clone();

        for replica in &replicas {
            if replica != &app_state.socket_address {
                let url = format!("http://{}/heartbeat", replica);
                let payload = json!({ "socket_address": app_state.socket_address });
                let payload_str = serde_json::to_string(&payload).unwrap_or_else(|_| "Failed to serialize".to_string());

                let response = client.put(&url).body(payload_str).send().await;
                match response {
                    Ok(response) => info!("[{}] Sent heartbeat to {}: {:?}", Instant::now().elapsed().as_millis(), replica, response),
                    Err(e) => error!("[{}] Error sending heartbeat to {}: {}", Instant::now().elapsed().as_millis(), replica, e),
                }
            }
        }
    }
}

#[put("/heartbeat")]
async fn heartbeat(req_body: String, data: web::Data<AppState>) -> impl Responder {
    let req: HeartbeatRequest = match serde_json::from_str(&req_body) {
        Ok(req) => req,
        Err(e) => return HttpResponse::BadRequest().body(format!("Invalid JSON: {}", e)),
    };

    let sender_address = &req.socket_address;
    if sender_address.is_empty() {
        return HttpResponse::BadRequest().body("Empty socket address");
    }

    // Process the heartbeat asynchronously
    tokio::spawn(process_heartbeat(data.clone(), sender_address.clone()));

    HttpResponse::Ok().finish()
}

async fn process_heartbeat(app_state: web::Data<AppState>, sender_address: String) {
    let current_time = Instant::now();

    {
        let mut last_heartbeat_received = app_state.last_heartbeat_received.write().await;
        last_heartbeat_received.insert(sender_address.clone(), current_time);
    }

    let mut view = app_state.view.lock().unwrap();
    if !view.contains(&sender_address) {
        view.push(sender_address.clone());
        log::info!("Replica {} re-added to view.", sender_address);
    }
    // No need for broadcast here as the view is updated internally
}

async fn process_view_removal(app_state: &web::Data<AppState>, replica: &String) {
    {
        // Restrict the scope of the MutexGuard cum
        {
            let mut view = app_state.view.lock().unwrap();
            view.retain(|r| r != replica);
        }

        {
            let mut last_heartbeat_received = app_state.last_heartbeat_received.write().await;
            last_heartbeat_received.remove(replica);
        }

        log::info!("Removing replica: {}", replica);
    }

    // Broadcasting view changes - this is now outside the scope of the MutexGuard cum
    broadcast_view_change("delete", replica, &app_state).await;
}


// Function to check a single replica's failure
async fn check_replica_failure(app_state: web::Data<AppState>, replica: String) {
    let heartbeat_timeout = Duration::from_millis(3000);
    let mut interval = tokio::time::interval(Duration::from_millis(1000));

    loop {
        interval.tick().await;
        let now = Instant::now();
        let last_heartbeat_time = {
            let last_heartbeat_received = app_state.last_heartbeat_received.read().await;
            last_heartbeat_received.get(&replica).cloned()
        };

        if let Some(last_time) = last_heartbeat_time {
            let elapsed = now.duration_since(last_time);
            if elapsed > heartbeat_timeout {
                info!("Heartbeat timeout for replica {}. Elapsed: {:?}", replica, elapsed);
                let mut view = app_state.view.lock().unwrap();
                if view.contains(&replica) {
                    view.retain(|r| r != &replica);
                    info!("Replica {} removed from view due to timeout.", replica);
                }
                break;
            } else {
                info!("Replica {} is alive. Elapsed since last heartbeat: {:?}", replica, elapsed);
            }
        }
    }
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

#[put("/view")]
async fn add_to_view(req: web::Json<ViewChangeRequest>, data: web::Data<AppState>) -> impl Responder {
    let new_replica = &req.socket_address;
    let mut view = data.view.lock().unwrap();
    log::info!("Before adding, view: {:?}", *view);

    if view.contains(new_replica) {
        HttpResponse::Ok().json(json!({ "result": "already present" }))
    } else {
        view.push(new_replica.clone());
        log::info!("After adding, view: {:?}", *view);
        broadcast_view_change("add", new_replica, &data).await;
        HttpResponse::Created().json(json!({ "result": "added" }))
    }
}

#[get("/view")]
async fn get_view(data: web::Data<AppState>) -> impl Responder {
    log::info!("Accessing current view");
    let view_clone = {
        let view = data.view.lock().unwrap();
        log::info!("Current view: {:?}", *view);
        view.clone()
    };
    HttpResponse::Ok().json(json!({ "view": view_clone }))
}

#[delete("/view")]
async fn remove_from_view(req: web::Json<ViewChangeRequest>, data: web::Data<AppState>) -> impl Responder {
    let replica_to_remove = &req.socket_address;
    let mut view = data.view.lock().unwrap();

    if let Some(index) = view.iter().position(|r| r == replica_to_remove) {
        view.remove(index);
        broadcast_view_change("delete", replica_to_remove, &data).await;
        HttpResponse::Ok().json(json!({ "result": "deleted" }))
    } else {
        HttpResponse::NotFound().json(json!({ "error": "View has no such replica" }))
    }
}

async fn broadcast_write(method: &str, key: &str, data: &KeyValue, app_state: &web::Data<AppState>) -> Result<(), reqwest::Error> {
    let client = Client::builder().timeout(Duration::from_secs(1)).build()?;
    let current_replica = &app_state.socket_address;

    for replica in &app_state.replicas {
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

async fn broadcast_view_change(action: &str, socket_address: &str, app_state: &web::Data<AppState>) {
    let client = reqwest::Client::new();
    let view_clone = {
        let view_guard = app_state.view.lock().unwrap();
        view_guard.clone()
    };

    for replica in view_clone.iter() {
        if replica != &app_state.socket_address {
            let url = format!("http://{}/internal/view", replica);
            let _ = client.put(&url)
                .json(&serde_json::json!({ "socket-address": socket_address, "action": action }))
                .send()
                .await;
        }
    }
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

#[put("/internal/view")]
async fn internal_view_update(req: web::Json<InternalViewChangeRequest>, data: web::Data<AppState>) -> impl Responder {
    let replica = &req.socket_address;
    let action = &req.action;

    log::info!("Internal view update request: {:?}", req);

    let mut view = data.view.lock().unwrap();

    match action.as_str() {
        "add" => {
            if !view.contains(replica) {
                log::info!("Adding replica to view: {}", replica);
                view.push(replica.clone());
            }
        },
        "delete" => {
            if view.contains(replica) {
                log::info!("Removing replica from view: {}", replica);
                view.retain(|r| r != replica);
            }
        },
        _ => log::warn!("Unknown action: {}", action),
    }

    HttpResponse::Ok()
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    env_logger::init();
    log::info!("Starting server...");


    // Initialize the view from the VIEW environment variable
    let view_str = env::var("VIEW").unwrap_or_default();
    let initial_view: Vec<String> = view_str.split(',').map(String::from).collect();
    log::info!("VIEW environment variable: {}", view_str);
    log::info!("Initial view: {:?}", initial_view);

    let initial_view_clone = initial_view.clone(); // Clone the initial_view

    let vector_clock = initialize_vector_clock(&initial_view);
    let last_heartbeat_received: HashMap<String, Instant> = initial_view.iter()
        .map(|replica| (replica.clone(), Instant::now()))
        .collect();

    let socket_address = env::var("SOCKET_ADDRESS").unwrap_or_else(|_| "localhost".to_string());
    let port = env::var("PORT").unwrap_or_else(|_| "8090".to_string());

    // Using the initial_view Vec directly for the dynamic list of active replicas
    let app_data = web::Data::new(AppState {
        store: Mutex::new(HashMap::new()),
        vector_clock: Arc::new(Mutex::new(vector_clock)),
        socket_address: socket_address.clone(),
        replicas: initial_view.clone(), // Static list of all possible replicas
        view: Arc::new(Mutex::new(initial_view)), // Dynamic list of active replicas
        last_heartbeat_received: Arc::new(RwLock::new(last_heartbeat_received)),
    });

    {
        let app_state_clone = app_data.clone();
        tokio::spawn(async move {
            send_heartbeats(app_state_clone).await;
        });
    }

    // Spawn separate tasks for each replica to check for failures
    for replica in initial_view_clone.iter() {
        if replica != &socket_address {
            let app_state_clone = app_data.clone();
            let replica_clone = replica.clone();
            tokio::spawn(async move {
                check_replica_failure(app_state_clone, replica_clone).await;
            });
        }
    }

    HttpServer::new(move || {
        App::new()
            .app_data(app_data.clone())
            .service(put)
            .service(get)
            .service(delete)
            .service(internal_put)
            .service(internal_delete)
            .service(add_to_view)
            .service(get_view)
            .service(remove_from_view)
            .service(internal_view_update)
            .service(heartbeat)
    })
    .bind(format!("0.0.0.0:{}", port))?
    .run()
    .await
}

fn initialize_vector_clock(replicas: &[String]) -> HashMap<String, u64> {
    replicas.iter().map(|replica| (replica.clone(), 0)).collect()
}
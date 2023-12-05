Temporary. Do not submit.
# CSE138_Assignment3_temp_rust
A basic HTTP web service with communicating instances implementing a replicated, fault-tolerant, causally consistent key-value store, with `/view` endpoints to manage participating replicas, and `/kvs` endpoints to perform key-value operations, supporting CRUD operations on keys like PUT to create/replace, GET to retrieve, and DELETE to remove key-value pairs, using vector clocks in the causal-metadata JSON fields to track causal dependencies, requiring writes to respect dependencies by returning errors before application, replicating writes to participating replicas, providing fault tolerance through replication, detecting crashed replicas by heartbeating, and ensuring eventual consistency.

## Team Contributions
- [Joey Ma](https://people.ucsc.edu/~jma363/)

## Acknowledgments

“The land on which we gather is the unceded territory of the Awaswas-speaking Uypi Tribe. The Amah Mutsun Tribal Band, comprised of the descendants of indigenous people taken to missions Santa Cruz and San Juan Bautista during Spanish colonization of the Central Coast, is today working hard to restore traditional stewardship practices on these lands and heal from historical trauma.”

I acknowledge that I reside on the land of the Amah Mutsun Tribal Band of the Ohlone people.

## Citations

- [Actix](https://actix.rs/) Web framework documentation
- [Land Acknowledgment](https://www.ucsc.edu/land-acknowledgment/) Statement at UC Santa Cruz
- [Reqwest](https://docs.rs/reqwest) Crate reqwest documentation
- [rust](https://hub.docker.com/_/rust) Docker image example Dockerfile
- [Serde JSON](https://docs.rs/serde_json) Crate serde_json documentation
- [Tokio](https://tokio.rs/) Tokio documentation

## Mechanism Description
### Causal Dependency Tracking
The system tracks causal dependencies between operations using a vector clock stored in the globalVectorClock variable, which is a JSON object mapping replica IDs to integers representing that replica's clock value. The clock values indicate when writes occurred and encode happens-before dependencies between writes. On every write, the local replica's clock is incremented. Clocks are attached to every write in the causal-metadata JSON field and merged on receive. Writes are held until their dependencies as indicated by causal-metadata are satisfied per the local vector clock.

### Down detection
Heartbeating is used to detect offline replicas. Every replica maintains a lastHeartbeatReceived map that tracks the last time a heartbeat request was received from each peer replica. Heartbeats are sent every second. The system checks if the time since last receiving a heartbeat has exceeded the configured timeout of 3 seconds, indicating an offline replica, and updates the view by broadcasting a request to delete the offline replica. When a new replica comes online, it announces its presence and retrieves state from peers.
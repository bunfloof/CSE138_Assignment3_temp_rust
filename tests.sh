### This is not a script and not meant to be run as is.

### Remember to enable RUST_LOG=debug parameter in docker commands, example:
docker run --publish=8082:8090 --net=asg3net --ip=10.10.0.2 --name=alice -e RUST_LOG=debug -e SOCKET_ADDRESS=10.10.0.2:8090 -e VIEW=10.10.0.2:8090,10.10.0.3:8090,10.10.0.4:8090 asg3img

### The <metadata> field should be replaced with the actual causal metadata returned from the previous PUT/GET requests.

## 💦 1 test_value_operations test
# Put moon:cake
curl -X PUT -H "Content-Type: application/json" -d '{"value":"cake","causal-metadata":null}' http://localhost:8082/kvs/moon

# Get moon:cake from alice 
curl -X GET -H "Content-Type: application/json" -d '{"causal-metadata":<metadata>}' http://localhost:8082/kvs/moon

# Get moon:cake from bob
curl -X GET -H "Content-Type: application/json" -d '{"causal-metadata":<metadata>}' http://localhost:8083/kvs/moon  

# Get moon:cake from carol
curl -X GET -H "Content-Type: application/json" -d '{"causal-metadata":<metadata>}' http://localhost:8084/kvs/moon

# Put moon:pie 
curl -X PUT -H "Content-Type: application/json" -d '{"value":"pie","causal-metadata":<metadata>}' http://localhost:8082/kvs/moon

# Get moon:pie from alice
curl -X GET -H "Content-Type: application/json" -d '{"causal-metadata":<metadata>}' http://localhost:8082/kvs/moon

# Get moon:pie from bob 
curl -X GET -H "Content-Type: application/json" -d '{"causal-metadata":<metadata>}' http://localhost:8083/kvs/moon

# Get moon:pie from carol
curl -X GET -H "Content-Type: application/json" -d '{"causal-metadata":<metadata>}' http://localhost:8084/kvs/moon

## 💦 2 test_availability test
docker network disconnect asg3net bob
docker network disconnect asg3net carol

curl -X PUT "http://localhost:8082/kvs/pop" -H "Content-Type: application/json" -d '{"value": "tarts", "causal-metadata": null}'

## 💦 3 test_view_changes test
# GET requests to /view for each replica:
curl -X GET http://localhost:8082/view
curl -X GET http://localhost:8083/view
curl -X GET http://localhost:8084/view

# PUT request to create apple:strudel:
curl -X PUT -H "Content-Type: application/json" -d '{"value":"strudel", "causal-metadata": null}' http://localhost:8082/kvs/apple

# GET requests to check apple:strudel at each replica:
curl -X GET -H "Content-Type: application/json" -d '{"causal-metadata":<metadata>}' http://localhost:8082/kvs/apple
curl -X GET -H "Content-Type: application/json" -d '{"causal-metadata":<metadata>}' http://localhost:8083/kvs/apple
curl -X GET -H "Content-Type: application/json" -d '{"causal-metadata":<metadata>}' http://localhost:8084/kvs/apple

# Docker command to kill the carol replica:
docker kill carol

# PUT request to create chocolate:eclair:
curl -X PUT -H "Content-Type: application/json" -d '{"value":"eclair", "causal-metadata": <metadata>}' http://localhost:8082/kvs/chocolate

# GET requests to check chocolate:eclair at remaining replicas:
curl -X GET -H "Content-Type: application/json" -d '{"causal-metadata":<metadata>}' http://localhost:8082/kvs/chocolate
curl -X GET -H "Content-Type: application/json" -d '{"causal-metadata":<metadata>}' http://localhost:8083/kvs/chocolate

# GET requests to /view for remaining replicas:
curl -X GET http://localhost:8082/view
curl -X GET http://localhost:8083/view

# Docker command to start the carol replica:
docker run --rm --detach --publish=8084:8090 --net=asg3net --ip=10.10.0.4 --name=carol -e=SOCKET_ADDRESS=10.10.0.4:8090 -e=VIEW=10.10.0.2:8090,10.10.0.3:8090,10.10.0.4:8090 asg3img

# GET requests to /view for each replica:
curl -X GET http://localhost:8082/view
curl -X GET http://localhost:8083/view
curl -X GET http://localhost:8084/view

# GET requests to check keys apple and chocolate at the carol replica:
curl -X GET -H "Content-Type: application/json" -d '{"causal-metadata":<metadata>}' http://localhost:8084/kvs/apple
curl -X GET -H "Content-Type: application/json" -d '{"causal-metadata":<metadata>}' http://localhost:8084/kvs/chocolate

## 💦 4 test_causal_consistency test
# Put matcha:ice-cream
curl -X PUT -H "Content-Type: application/json" -d '{"value":"ice-cream","causal-metadata":null}' http://localhost:8082/kvs/matcha

# Disconnect bob
docker network disconnect asg3net bob

# Put matcha:tea
curl -X PUT -H "Content-Type: application/json" -d '{"value":"tea","causal-metadata":<metadata>}' http://localhost:8082/kvs/matcha  

# Connect bob
docker network connect asg3net bob

# Get matcha from alice  
curl -X GET -H "Content-Type: application/json" -d '{"causal-metadata":<metadata>}' http://localhost:8082/kvs/matcha

# Get matcha from bob (fails)
curl -X GET -H "Content-Type: application/json" -d '{"causal-metadata":<metadata>}' http://localhost:8083/kvs/matcha

# Put matcha:mochi at bob (fails)  
curl -X PUT -H "Content-Type: application/json" -d '{"value":"mochi","causal-metadata":<metadata>}' http://localhost:8083/kvs/matcha

# Get matcha from bob with previous metadata
curl -X GET -H "Content-Type: application/json" -d '{"causal-metadata":<previous_metadata>}' http://localhost:8083/kvs/matcha

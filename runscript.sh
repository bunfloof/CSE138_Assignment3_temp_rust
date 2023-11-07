docker stop alice bob carol
docker rm alice bob carol
docker network rm asg3net

docker network create --subnet=10.10.0.0/16 asg3net
docker build -t asg3img .

docker run --rm -d --publish=8082:8090 --net=asg3net --ip=10.10.0.2 --name=alice -e RUST_LOG=debug -e SOCKET_ADDRESS=10.10.0.2:8090 -e VIEW=10.10.0.2:8090,10.10.0.3:8090,10.10.0.4:8090 asg3img
docker run --rm -d --publish=8083:8090 --net=asg3net --ip=10.10.0.3 --name=bob -e RUST_LOG=debug -e SOCKET_ADDRESS=10.10.0.3:8090 -e VIEW=10.10.0.2:8090,10.10.0.3:8090,10.10.0.4:8090 asg3img
docker run --rm -d --publish=8084:8090 --net=asg3net --ip=10.10.0.4 --name=carol -e RUST_LOG=debug -e SOCKET_ADDRESS=10.10.0.4:8090 -e VIEW=10.10.0.2:8090,10.10.0.3:8090,10.10.0.4:8090 asg3img


# DO NOT UNCOMMENT
# docker run --publish=8082:8090 --net=asg3net --ip=10.10.0.2 --name=alice -e RUST_LOG=debug -e SOCKET_ADDRESS=10.10.0.2:8090 -e VIEW=10.10.0.2:8090,10.10.0.3:8090,10.10.0.4:8090 asg3img
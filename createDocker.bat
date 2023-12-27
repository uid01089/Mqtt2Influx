pip freeze > requirements.txt
docker build -t mqtt2influx -f Dockerfile .
docker tag mqtt2influx:latest docker.diskstation/mqtt2influx
docker push docker.diskstation/mqtt2influx:latest
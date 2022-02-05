docker-compose down --volume --remove-orphans

docker build --no-cache . -f Dockerfile --tag airflow-raizen-challenge:0.0.1
docker-compose up -d
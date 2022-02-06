CURRENT_DIRECTORY=`pwd`
echo "DATALAKE_PATH="$CURRENT_DIRECTORY > .env
mkdir raw_zone stage_zone refined_zone dags logs plugins
chmod -R 777 raw_zone stage_zone refined_zone dags logs plugins
docker build --no-cache . -f Dockerfile --tag airflow-raizen-challenge:0.0.1
docker-compose up -d
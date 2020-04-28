sudo yum update -y
sudo yum install -y docker
sudo service docker start
docker pull viswamohanty/airflow-demo:latest
docker container run -itd --rm -p 8080:8080 --name airflow_demo viswamohanty/airflow-demo


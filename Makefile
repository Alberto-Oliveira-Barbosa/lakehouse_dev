up:
	docker compose up --build -d

down:
	docker compose down

logs:
	docker compose logs -f


logs_airflow:
	docker compose logs airflow


logs_postegres:
	docker compose logs postgres


logs_minio:
	docker compose logs minio


build:
	docker compose build --no-cache
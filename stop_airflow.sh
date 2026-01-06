#!/bin/bash

echo "=========================================="
echo "Stopping Services..."
echo "=========================================="

# Stop Airflow
pkill -f "airflow standalone" && echo "✅ Airflow stopped" || echo "  No Airflow running"
pkill -f "airflow scheduler" && echo "✅ Scheduler stopped" || true
pkill -f "airflow webserver" && echo "✅ Webserver stopped" || true

# Stop ALL MinIO processes
pkill -9 -f "minio server" && echo "✅ MinIO stopped" || echo "  No MinIO running"

echo ""
echo "✅ All services stopped"
echo "=========================================="
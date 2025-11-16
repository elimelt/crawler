#!/bin/bash
set -e

if [ $# -eq 0 ]; then
    echo "Error: No URL provided"
    echo "Usage: $0 <URL>"
    echo "Example: $0 https://example.com"
    exit 1
fi

URL="$1"

DOMAIN=$(echo "$URL" | sed -E 's|^https?://||' | sed -E 's|/.*$||' | sed 's/:.*$//')
SAFE_DOMAIN=$(echo "$DOMAIN" | tr '.' '_' | tr ':' '_')
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
CONTAINER_NAME="crawler_${SAFE_DOMAIN}_${TIMESTAMP}"
DB_FILE="data/${SAFE_DOMAIN}_${TIMESTAMP}.db"
JSONL_FILE="data/${SAFE_DOMAIN}_${TIMESTAMP}.jsonl"

echo "=========================================="
echo "Starting Unlimited Crawl"
echo "=========================================="
echo "URL:           $URL"
echo "Domain:        $DOMAIN"
echo "Container:     $CONTAINER_NAME"
echo "Database:      $DB_FILE"
echo "Output:        $JSONL_FILE"
echo "=========================================="
echo ""

if ! docker images | grep -q "^crawler "; then
    echo "Building Docker image..."
    docker build -t crawler:latest .
    echo ""
fi

mkdir -p data

if docker ps -a | grep -q "$CONTAINER_NAME"; then
    echo "Removing existing container: $CONTAINER_NAME"
    docker rm -f "$CONTAINER_NAME" 2>/dev/null || true
    echo ""
fi

echo "Starting crawl in Docker container..."
echo "Press Ctrl+C to stop the crawl (container will continue running in background)"
echo ""

docker run -d \
    --name "$CONTAINER_NAME" \
    -v "$(pwd)/data:/app/data" \
    -p 8000:8000 \
    crawler:latest \
    python extract_with_metrics.py \
    --start "$URL" \
    --allowed-domain "$DOMAIN" \
    --max-pages 999999999 \
    --max-depth 999 \
    --concurrency 16 \
    --delay 0.3 \
    --timeout 30.0 \
    --out "/app/$JSONL_FILE" \
    --sqlite "/app/$DB_FILE" \
    --prometheus-port 8000 \
    --metrics-interval 30 \
    --max-connections 32 \
    -v

echo "Container started: $CONTAINER_NAME"
echo ""
echo "=========================================="
echo "Monitoring & Control"
echo "=========================================="
echo "View logs:         docker logs -f $CONTAINER_NAME"
echo "Stop crawl:        docker stop $CONTAINER_NAME"
echo "Remove container:  docker rm $CONTAINER_NAME"
echo "Metrics endpoint:  http://localhost:8000/metrics"
echo ""
echo "Database:          $DB_FILE"
echo "Output:            $JSONL_FILE"
echo "=========================================="
echo ""

echo "Following logs (Ctrl+C to detach, container will keep running)..."
echo ""
docker logs -f "$CONTAINER_NAME"


#!/bin/bash
set -e

if [ $# -lt 2 ]; then
    echo "Error: Missing arguments"
    echo "Usage: $0 <URL> <SQLITE_DB_PATH>"
    echo "Example: $0 https://example.com /absolute/path/to/data/example_20250101_120000.db"
    exit 1
fi

URL="$1"
SQLITE_PATH="$2"

if [ ! -f "$SQLITE_PATH" ]; then
    echo "Error: SQLite file not found: $SQLITE_PATH" >&2
    exit 1
fi

DOMAIN=$(echo "$URL" | sed -E 's|^https?://||' | sed -E 's|/.*$||' | sed 's/:.*$//')
SAFE_DOMAIN=$(echo "$DOMAIN" | tr '.' '_' | tr ':' '_')
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
CONTAINER_NAME="crawler_resume_${SAFE_DOMAIN}_${TIMESTAMP}"

HOST_DATA_DIR=$(cd "$(dirname "$SQLITE_PATH")" && pwd)
SQLITE_BASENAME=$(basename "$SQLITE_PATH")
JSONL_BASENAME=$(echo "$SQLITE_BASENAME" | sed -E 's/\.db$/.jsonl/')
JSONL_HOST_PATH="${HOST_DATA_DIR}/${JSONL_BASENAME}"

function is_port_free() {
    local port="$1"
    if command -v ss > /dev/null 2>&1; then
        if ss -ltnH 2>/dev/null | awk '{print $4}' | grep -qE ":${port}$"; then
            return 1
        else
            return 0
        fi
    fi
    if command -v netstat > /dev/null 2>&1; then
        if netstat -ltn 2>/dev/null | awk '{print $4}' | grep -qE ":${port}$"; then
            return 1
        else
            return 0
        fi
    fi
    if command -v python3 > 0 2>&1; then
        python3 - <<EOF
import socket, sys
port = int("${port}")
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
try:
    s.bind(("0.0.0.0", port))
    s.close()
    sys.exit(0)
except OSError:
    sys.exit(1)
EOF
        return $?
    fi
    return 0
}

function scan_for_available_ports() {
    for port in {8000..8010}; do
        if is_port_free "$port"; then
            echo "$port"
            return 0
        fi
    done
    return 1
}

echo "Scanning for available port..."
if ! AVAILABLE_PORT=$(scan_for_available_ports); then
    echo "Error: No available port found in range 8000-8010" >&2
    exit 1
fi
echo "Available port: $AVAILABLE_PORT"

echo "=========================================="
echo "Resuming Crawl"
echo "=========================================="
echo "URL:           $URL"
echo "Domain:        $DOMAIN"
echo "Container:     $CONTAINER_NAME"
echo "SQLite (host): $SQLITE_PATH"
echo "Port:          $AVAILABLE_PORT"
echo "=========================================="
echo ""

if ! docker images | grep -q "^crawler "; then
    echo "Building Docker image..."
    docker build -t crawler:latest .
    echo ""
fi

if docker ps -a | grep -q "$CONTAINER_NAME"; then
    echo "Removing existing container: $CONTAINER_NAME"
    docker rm -f "$CONTAINER_NAME" 2>/dev/null || true
    echo ""
fi

echo "Starting resume in Docker container..."
echo ""

docker run -d \
    --name "$CONTAINER_NAME" \
    -v "$HOST_DATA_DIR:/app/data" \
    -p $AVAILABLE_PORT:8000 \
    crawler:latest \
    python extract_with_metrics.py \
    --start "$URL" \
    --allowed-domain "$DOMAIN" \
    --max-pages 999999999 \
    --max-depth 99999 \
    --concurrency ${CONCURRENCY:-64} \
    --delay ${DELAY:-0.05} \
    --timeout ${TIMEOUT:-20.0} \
    --ignore-robots \
    --out "/app/data/$JSONL_BASENAME" \
    --sqlite "/app/data/$SQLITE_BASENAME" \
    --resume \
    --prometheus-port 8000 \
    --metrics-interval 30 \
    --max-connections ${MAX_CONNECTIONS:-128} \
    -vv

echo "Container started: $CONTAINER_NAME"
echo ""
echo "=========================================="
echo "Monitoring & Control"
echo "=========================================="
echo "View logs:         docker logs -f $CONTAINER_NAME"
echo "Stop crawl:        docker stop $CONTAINER_NAME"
echo "Remove container:  docker rm $CONTAINER_NAME"
echo "Metrics endpoint:  http://localhost:$AVAILABLE_PORT/metrics"
echo ""
echo "SQLite DB:         $SQLITE_PATH"
echo "Output JSONL:      $JSONL_HOST_PATH"
echo "=========================================="
echo ""

echo "Following logs (Ctrl+C to detach, container will keep running)..."
echo ""
docker logs -f "$CONTAINER_NAME"



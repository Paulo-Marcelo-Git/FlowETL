#!/usr/bin/env bash
# Gerencia o watcher FlowETL em ambiente de desenvolvimento.
# Uso: ./scripts/watcher.sh {start|stop|restart|status|logs}

set -euo pipefail

BASE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VENV="$BASE_DIR/.venv/bin/python"
PID_FILE="$BASE_DIR/logs/watcher.pid"
LOG_FILE="$BASE_DIR/logs/watcher.log"

mkdir -p "$BASE_DIR/logs"

_pid() {
    [[ -f "$PID_FILE" ]] && cat "$PID_FILE" || echo ""
}

_running() {
    local pid
    pid=$(_pid)
    [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null
}

cmd_start() {
    if _running; then
        echo "Watcher já está rodando (PID $(_pid))."
        exit 0
    fi
    nohup "$VENV" -m bot.watcher >> "$LOG_FILE" 2>&1 &
    echo $! > "$PID_FILE"
    echo "Watcher iniciado (PID $(cat "$PID_FILE")). Logs em logs/watcher.log"
}

cmd_stop() {
    if ! _running; then
        echo "Watcher não está rodando."
        rm -f "$PID_FILE"
        exit 0
    fi
    kill "$(_pid)"
    rm -f "$PID_FILE"
    echo "Watcher encerrado."
}

cmd_status() {
    if _running; then
        echo "Watcher rodando (PID $(_pid))."
    else
        echo "Watcher parado."
        rm -f "$PID_FILE"
    fi
}

cmd_logs() {
    tail -f "$LOG_FILE"
}

case "${1:-}" in
    start)   cmd_start   ;;
    stop)    cmd_stop    ;;
    restart) cmd_stop; sleep 1; cmd_start ;;
    status)  cmd_status  ;;
    logs)    cmd_logs    ;;
    *)
        echo "Uso: $0 {start|stop|restart|status|logs}"
        exit 1
        ;;
esac

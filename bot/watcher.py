"""
Watcher: monitora a pasta_monitorada/ com watchdog e dispara o ETL
quando um arquivo .xlsx é criado ou movido para lá.
"""

import os
import signal
import sys
import time
from pathlib import Path

from dotenv import load_dotenv
from watchdog.events import FileSystemEventHandler, FileCreatedEvent, FileMovedEvent
from watchdog.observers import Observer

from bot.alertas import iniciar_scheduler_relatorio, parar_scheduler
from bot.etl import processar_arquivo
from bot.logger import configurar_logger

load_dotenv()

logger = configurar_logger(__name__)

BASE_DIR = Path(__file__).resolve().parent.parent
PASTA_MONITORADA = BASE_DIR / 'pasta_monitorada'

# Segundos de espera para garantir que o arquivo foi completamente copiado
DELAY_ESTABILIZACAO = 3


class ExcelHandler(FileSystemEventHandler):
    """Reage a eventos de criação/movimentação de arquivos .xlsx."""

    def _deve_processar(self, caminho: str) -> bool:
        return caminho.lower().endswith(('.xlsx', '.xls'))

    def _aguardar_estabilizacao(self, caminho: str) -> bool:
        """Espera até o arquivo parar de crescer (upload/cópia concluída)."""
        tamanho_anterior = -1
        for _ in range(10):
            try:
                tamanho_atual = os.path.getsize(caminho)
            except OSError:
                return False
            if tamanho_atual == tamanho_anterior and tamanho_atual > 0:
                return True
            tamanho_anterior = tamanho_atual
            time.sleep(DELAY_ESTABILIZACAO)
        return True

    def on_created(self, event: FileCreatedEvent) -> None:
        if event.is_directory or not self._deve_processar(event.src_path):
            return
        logger.info(f'Novo arquivo detectado: {event.src_path}')
        self._aguardar_estabilizacao(event.src_path)
        processar_arquivo(event.src_path)

    def on_moved(self, event: FileMovedEvent) -> None:
        if event.is_directory or not self._deve_processar(event.dest_path):
            return
        logger.info(f'Arquivo movido para pasta monitorada: {event.dest_path}')
        self._aguardar_estabilizacao(event.dest_path)
        processar_arquivo(event.dest_path)


def iniciar_watcher() -> None:
    """Inicia o observer watchdog e mantém o processo rodando."""
    PASTA_MONITORADA.mkdir(parents=True, exist_ok=True)

    handler = ExcelHandler()
    observer = Observer()
    observer.schedule(handler, str(PASTA_MONITORADA), recursive=False)
    observer.start()

    iniciar_scheduler_relatorio()

    logger.info(f'FlowETL iniciado. Monitorando: {PASTA_MONITORADA}')
    logger.info('Pressione Ctrl+C para encerrar.')

    def _parar(signum, frame):
        logger.info('Sinal de encerramento recebido. Parando...')
        observer.stop()
        parar_scheduler()
        sys.exit(0)

    signal.signal(signal.SIGINT, _parar)
    signal.signal(signal.SIGTERM, _parar)

    try:
        while observer.is_alive():
            observer.join(timeout=1)
    finally:
        observer.stop()
        observer.join()
        parar_scheduler()
        logger.info('FlowETL encerrado.')


if __name__ == '__main__':
    iniciar_watcher()

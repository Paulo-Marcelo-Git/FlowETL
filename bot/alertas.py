"""
Módulo de alertas:
- Telegram: disparo imediato em caso de falha
- Email: relatório diário às 8h via APScheduler
"""

import os
import smtplib
from datetime import datetime, date
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import List, Optional

import requests
from apscheduler.schedulers.background import BackgroundScheduler
from dotenv import load_dotenv
from sqlalchemy import text

from bot.logger import configurar_logger

load_dotenv()

logger = configurar_logger(__name__)

# ------------------------------------------------------------------ Telegram

def enviar_telegram(mensagem: str) -> None:
    """Envia mensagem de texto via Telegram Bot API."""
    token = os.getenv('TELEGRAM_TOKEN')
    chat_id = os.getenv('TELEGRAM_CHAT_ID')

    if not token or not chat_id:
        logger.warning('TELEGRAM_TOKEN ou TELEGRAM_CHAT_ID não configurados — alerta ignorado.')
        return

    url = f'https://api.telegram.org/bot{token}/sendMessage'
    payload = {
        'chat_id': chat_id,
        'text': mensagem,
        'parse_mode': 'HTML',
    }

    try:
        resp = requests.post(url, json=payload, timeout=10)
        resp.raise_for_status()
        logger.info('Alerta Telegram enviado com sucesso.')
    except Exception as exc:
        logger.error(f'Falha ao enviar alerta Telegram: {exc}')


def alerta_falha_telegram(nm_arquivo: str, erro: str) -> None:
    """Formata e envia alerta de falha no processamento."""
    agora = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    mensagem = (
        '<b>❌ FlowETL — Falha no processamento</b>\n'
        f'📄 Arquivo: <code>{nm_arquivo}</code>\n'
        f'🔴 Erro: {erro}\n'
        f'⏱️ Horário: {agora}'
    )
    enviar_telegram(mensagem)


def alerta_sucesso_telegram(nm_arquivo: str, qt_linhas: int) -> None:
    """Formata e envia confirmação de processamento bem-sucedido."""
    agora = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    mensagem = (
        '<b>✅ FlowETL — Processamento concluído</b>\n'
        f'📄 Arquivo: <code>{nm_arquivo}</code>\n'
        f'📊 Linhas inseridas: {qt_linhas}\n'
        f'⏱️ Horário: {agora}'
    )
    enviar_telegram(mensagem)


# -------------------------------------------------------------------- Email

def _enviar_email(assunto: str, corpo_html: str) -> None:
    """Envia e-mail HTML via SMTP."""
    remetente = os.getenv('EMAIL_USER')
    senha = os.getenv('EMAIL_PASS')
    destinatario = os.getenv('EMAIL_DESTINATARIO')
    smtp_server = os.getenv('EMAIL_SMTP', 'smtp.gmail.com')
    smtp_porta = int(os.getenv('EMAIL_PORTA', '587'))

    if not all([remetente, senha, destinatario]):
        logger.warning('Configuração de e-mail incompleta — alerta ignorado.')
        return

    msg = MIMEMultipart('alternative')
    msg['Subject'] = assunto
    msg['From'] = remetente
    msg['To'] = destinatario
    msg.attach(MIMEText(corpo_html, 'html', 'utf-8'))

    try:
        with smtplib.SMTP(smtp_server, smtp_porta, timeout=15) as server:
            server.ehlo()
            server.starttls()
            server.login(remetente, senha)
            server.sendmail(remetente, [destinatario], msg.as_string())
        logger.info(f'E-mail enviado para {destinatario}.')
    except Exception as exc:
        logger.error(f'Falha ao enviar e-mail: {exc}')


def enviar_relatorio_diario(resumo: Optional[List[dict]] = None) -> None:
    """Gera e envia o relatório diário de execuções."""
    hoje = datetime.now().strftime('%d/%m/%Y')
    assunto = f'✅ FlowETL — Relatório Diário {hoje}'

    linhas_tabela = ''
    total_recebidas = total_inseridas = total_falhas = 0

    if resumo:
        for item in resumo:
            status_badge = '✅' if item.get('ds_status') == 'sucesso' else '❌'
            linhas_tabela += (
                f'<tr>'
                f'<td>{item.get("nm_arquivo", "")}</td>'
                f'<td>{item.get("nm_tabela_destino", "")}</td>'
                f'<td>{item.get("qt_linhas_recebidas", 0)}</td>'
                f'<td>{item.get("qt_linhas_inseridas", 0)}</td>'
                f'<td>{item.get("qt_linhas_rejeitadas", 0)}</td>'
                f'<td>{status_badge} {item.get("ds_status", "")}</td>'
                f'</tr>'
            )
            total_recebidas += item.get('qt_linhas_recebidas') or 0
            total_inseridas += item.get('qt_linhas_inseridas') or 0
            total_falhas += 1 if item.get('ds_status') != 'sucesso' else 0

    corpo_html = f"""
    <html><body style="font-family: Arial, sans-serif; color: #333;">
      <h2>📊 FlowETL — Relatório Diário {hoje}</h2>
      <p>Resumo das execuções de hoje:</p>
      <table border="1" cellpadding="6" cellspacing="0" style="border-collapse:collapse; width:100%;">
        <thead style="background:#4A90D9; color:white;">
          <tr>
            <th>Arquivo</th><th>Tabela</th><th>Recebidas</th>
            <th>Inseridas</th><th>Rejeitadas</th><th>Status</th>
          </tr>
        </thead>
        <tbody>
          {linhas_tabela if linhas_tabela else '<tr><td colspan="6">Nenhuma execução registrada hoje.</td></tr>'}
        </tbody>
        <tfoot style="background:#f0f0f0; font-weight:bold;">
          <tr>
            <td colspan="2">TOTAL</td>
            <td>{total_recebidas}</td><td>{total_inseridas}</td>
            <td>—</td><td>{'✅ OK' if total_falhas == 0 else f'❌ {total_falhas} falha(s)'}</td>
          </tr>
        </tfoot>
      </table>
      <br/>
      <small>FlowETL — Pipeline Excel → SQL Server → Metabase</small>
    </body></html>
    """
    _enviar_email(assunto, corpo_html)


# -------------------------------------------------------- Agendador diário

_scheduler: Optional[BackgroundScheduler] = None


def _buscar_resumo_diario() -> List[dict]:
    """Consulta tb_log_etl e retorna execuções do dia corrente."""
    try:
        from bot.database import obter_engine  # import local para evitar ciclo na inicialização
        engine = obter_engine()
        sql = text("""
            SELECT
                nm_arquivo,
                nm_tabela_destino,
                qt_linhas_recebidas,
                qt_linhas_inseridas,
                qt_linhas_rejeitadas,
                ds_status,
                ds_erro,
                tm_duracao_seg
            FROM dbo.tb_log_etl
            WHERE CAST(dt_processamento AS DATE) = CAST(GETDATE() AS DATE)
            ORDER BY dt_processamento
        """)
        with engine.connect() as conn:
            rows = conn.execute(sql).mappings().all()
        return [dict(r) for r in rows]
    except Exception as exc:
        logger.error(f'Falha ao buscar resumo diário para e-mail: {exc}')
        return []


def _enviar_relatorio_agendado() -> None:
    resumo = _buscar_resumo_diario()
    enviar_relatorio_diario(resumo)


def iniciar_scheduler_relatorio() -> None:
    """Inicia o APScheduler para envio do relatório às 8h."""
    global _scheduler

    if _scheduler and _scheduler.running:
        return

    _scheduler = BackgroundScheduler(timezone='America/Sao_Paulo')
    _scheduler.add_job(
        func=_enviar_relatorio_agendado,
        trigger='cron',
        hour=8,
        minute=0,
        id='relatorio_diario',
        replace_existing=True,
    )
    _scheduler.start()
    logger.info('Scheduler de relatório diário iniciado (08:00 BRT).')


def parar_scheduler() -> None:
    global _scheduler
    if _scheduler and _scheduler.running:
        _scheduler.shutdown(wait=False)
        logger.info('Scheduler de relatório diário encerrado.')

from datetime import datetime, timedelta
import pytest
from sqlalchemy.exc import OperationalError
from bot.retry import classificar_erro, _calcular_proxima_tentativa


class TestClassificarErro:
    def test_operational_error_e_infra(self):
        exc = OperationalError("connection refused", None, None)
        assert classificar_erro(exc) == 'infra'

    def test_timeout_error_e_infra(self):
        assert classificar_erro(TimeoutError()) == 'infra'

    def test_connection_error_e_infra(self):
        assert classificar_erro(ConnectionError()) == 'infra'

    def test_os_error_e_infra(self):
        assert classificar_erro(OSError()) == 'infra'

    def test_file_not_found_e_dado(self):
        # FileNotFoundError é subclasse de OSError — deve ser 'dado'
        assert classificar_erro(FileNotFoundError()) == 'dado'

    def test_value_error_e_dado(self):
        assert classificar_erro(ValueError('coluna não encontrada')) == 'dado'

    def test_key_error_e_dado(self):
        assert classificar_erro(KeyError('numero')) == 'dado'

    def test_keyword_timeout_na_mensagem_e_infra(self):
        assert classificar_erro(Exception('TCP timeout reached')) == 'infra'

    def test_keyword_connection_na_mensagem_e_infra(self):
        assert classificar_erro(Exception('connection reset by peer')) == 'infra'

    def test_keyword_unavailable_na_mensagem_e_infra(self):
        assert classificar_erro(Exception('server unavailable')) == 'infra'

    def test_erro_generico_e_dado(self):
        assert classificar_erro(Exception('coluna x nao existe')) == 'dado'


class TestCalcularProximaTentativa:
    def _delta(self, resultado: datetime) -> timedelta:
        return resultado - datetime.now()

    def test_infra_tentativa_0_e_1_minuto(self):
        delta = self._delta(_calcular_proxima_tentativa('infra', 0))
        assert timedelta(seconds=55) < delta < timedelta(seconds=65)

    def test_infra_tentativa_1_e_5_minutos(self):
        delta = self._delta(_calcular_proxima_tentativa('infra', 1))
        assert timedelta(minutes=4, seconds=55) < delta < timedelta(minutes=5, seconds=5)

    def test_infra_tentativa_2_e_30_minutos(self):
        delta = self._delta(_calcular_proxima_tentativa('infra', 2))
        assert timedelta(minutes=29, seconds=55) < delta < timedelta(minutes=30, seconds=5)

    def test_infra_tentativa_3_e_2_horas(self):
        delta = self._delta(_calcular_proxima_tentativa('infra', 3))
        assert timedelta(hours=1, minutes=59, seconds=55) < delta < timedelta(hours=2, seconds=5)

    def test_infra_tentativa_4_e_8_horas(self):
        delta = self._delta(_calcular_proxima_tentativa('infra', 4))
        assert timedelta(hours=7, minutes=59, seconds=55) < delta < timedelta(hours=8, seconds=5)

    def test_indice_alem_do_limite_usa_ultimo_backoff(self):
        # tentativa=99 → usa backoff[-1] = 480 min (8h)
        delta = self._delta(_calcular_proxima_tentativa('infra', 99))
        assert timedelta(hours=7, minutes=59, seconds=55) < delta < timedelta(hours=8, seconds=5)

    def test_dado_tentativa_0_e_5_minutos(self):
        delta = self._delta(_calcular_proxima_tentativa('dado', 0))
        assert timedelta(minutes=4, seconds=55) < delta < timedelta(minutes=5, seconds=5)

    def test_dado_tentativa_1_e_60_minutos(self):
        delta = self._delta(_calcular_proxima_tentativa('dado', 1))
        assert timedelta(minutes=59, seconds=55) < delta < timedelta(minutes=60, seconds=5)

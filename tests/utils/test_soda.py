from unittest.mock import MagicMock, patch

from historical_flights_airport_gym.utils.quality.check import SodaAnalyzer


@patch("historical_flights_airport_gym.utils.quality.check.soda.scan.Scan")
def test_funcao_retorna_status_e_dicionario_dq_check(mock_get):
    mock_response = MagicMock()
    mock_response.execute.return_value = 0
    mock_response.get_scan_results.return_value = {"hasFailures": False, "checks": []}
    mock_get.return_value = mock_response

    analyzer = SodaAnalyzer(conn=MagicMock())
    exit_code, result = analyzer.run_scan("dummy_path.yaml")

    assert isinstance(exit_code, int)
    assert isinstance(result, dict)
    assert exit_code == 0
    assert "hasFailures" in result

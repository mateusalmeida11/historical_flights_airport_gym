from soda.scan import Scan


class SodaAnalyzer:
    def __init__(self, conn):
        self.conn = conn

    def run_scan(self, checks_path):
        scan = Scan()
        scan.set_verbose(True)
        scan.add_duckdb_connection(self.conn)
        scan.set_data_source_name("duckdb")
        scan.add_sodacl_yaml_files(checks_path)
        exit_code = scan.execute()
        scan_results = scan.get_scan_results()
        return exit_code, scan_results

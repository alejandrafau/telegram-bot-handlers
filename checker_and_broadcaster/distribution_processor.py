import requests
import json
import time
from typing import Dict, List
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import logging

logger = logging.getLogger(__name__)
class DistributionProcessor:
    def __init__(self, max_workers: int = 10, delay: float = 0.1, max_retries: int = 2):
        self.max_workers = max_workers
        self.delay = delay
        self.max_retries = max_retries

    def _calculate_distribution_size(self, distribution_id: str, distribution: Dict) -> List:
        """
        Función que calcula la cantidad de filas/registros de un csv o un json
        Returns: [distribution_id, url, size, error]
        """
        url = distribution.get("url", "")
        size = None
        error = None

        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        }

        attempts = 0
        while attempts <= self.max_retries:
            try:
                with requests.get(url, headers=headers, verify=False, timeout=30, stream=True) as response:
                    if response.status_code != 200:
                        error = f"HTTP {response.status_code}"
                        return [distribution_id, url, size, error]

                    content_type = response.headers.get('Content-Type', '').lower()

                    # CSV / text
                    if 'csv' in content_type or url.endswith(('.csv', '.txt')):
                        try:
                            size = sum(1 for _ in response.iter_lines() if _)
                        except Exception as e:
                            error = f"CSV/Text parsing error: {str(e)}"

                    # JSON
                    elif 'json' in content_type or url.endswith('.json'):
                        try:
                            text = response.content.decode('utf-8', errors='ignore')
                            data = json.loads(text)
                            if isinstance(data, list):
                                size = len(data)
                            elif isinstance(data, dict):
                                array_keys = [k for k, v in data.items() if isinstance(v, list)]
                                size = len(data[array_keys[0]]) if array_keys else 1
                            else:
                                error = f"Unexpected JSON structure: {type(data)}"
                        except json.JSONDecodeError:
                            # fallback to JSON lines
                            try:
                                with requests.get(url, headers=headers, verify=False, timeout=30, stream=True) as r2:
                                    size = sum(1 for line in r2.iter_lines() if line.strip().startswith(b"{"))
                            except Exception as e:
                                error = f"JSON parsing error: {str(e)}"

                    else:
                        error = f"Unsupported content type: {content_type}"

                # success
                if size is not None:
                    return [distribution_id, url, size, error]

            except requests.exceptions.Timeout:
                error = "Request timeout"
            except requests.exceptions.ConnectionError:
                error = "Connection error"
            except requests.exceptions.RequestException as e:
                error = f"Request error: {str(e)}"
            except Exception as e:
                error = f"Unexpected error: {str(e)}"

            # retry logic
            attempts += 1
            if attempts <= self.max_retries:
                logger.warning(f"Retry {attempts} for {distribution_id} after error: {error}")
                time.sleep(1)

        # final fallback if all retries fail
        return [distribution_id, url, size, error or f"Failed after {self.max_retries} retries"]

    def process_distributions_concurrent(self, distributions: Dict[str, Dict]) -> List[List]:
        """
        Funcion que corre calculate_distribution_size en paralelo para las distribuciones listadas
        """
        results = []
        total = len(distributions)

        def worker(item):
            dist_id, dist_data = item
            logger.info(f"Processing {dist_id} - {dist_data.get('url', '')}")
            result = self._calculate_distribution_size(dist_id, dist_data)
            time.sleep(self.delay)
            return result

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {executor.submit(worker, item): item for item in distributions.items()}
            for future in tqdm(as_completed(futures), total=total, desc="Processing distributions"):
                results.append(future.result())

        return results
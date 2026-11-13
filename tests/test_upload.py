import sys
from pathlib import Path
import unittest
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
import upload

class CheckpointTests(unittest.TestCase):
    def test_rejects_modified_sources(self):
        source = [{'name': 'bangkok_2024-01.csv', 'sha256': 'abc'}]
        state = {'version': 1, 'next_index': 100, 'source_files': source}
        self.assertEqual(upload.checked_checkpoint(state, source), 100)
        with self.assertRaises(ValueError):
            upload.checked_checkpoint(state, [{'name': 'bangkok_2024-01.csv', 'sha256': 'changed'}])
        with self.assertRaises(ValueError):
            upload.checked_checkpoint({**state, 'next_index': -1}, source)

if __name__ == '__main__':
    unittest.main()

class DownloadFailureTests(unittest.TestCase):
    def test_retries_failure_and_reports_attempts(self):
        from unittest.mock import patch
        item = {'url': 'https://example.invalid/photo', 'ticket_id': 'T1', 'field': 'photo', 'source_csv': 'bangkok.csv'}
        with patch('pilot.urllib.request.urlopen', side_effect=TimeoutError('timeout')) as get, patch('pilot.time.sleep'):
            record, data = upload.download(item, timeout=12, attempts=2)
        self.assertIsNone(data)
        self.assertEqual(record['status'], 'failed')
        self.assertEqual(record['attempts'], 2)
        self.assertEqual(record['ticket_id'], 'T1')
        self.assertEqual(get.call_count, 2)
        self.assertEqual(get.call_args.kwargs['timeout'], 12)

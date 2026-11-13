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

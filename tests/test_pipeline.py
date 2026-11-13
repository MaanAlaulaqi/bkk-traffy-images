import json
from pathlib import Path
import sys
import tempfile
import unittest
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
import pipeline

class PipelineQueueTest(unittest.TestCase):
    def test_ready_batches_sorted_and_queue_bytes(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            for i in (20, 0, 10):
                d = root / f'ready-{i:012d}-id'; d.mkdir()
                (d / 'queue.json').write_text(json.dumps({'start_row': i}))
                (d / 'batch.parquet').write_bytes(b'hello')
            self.assertEqual([i for i, _, _ in pipeline.ready_batches(root)], [0, 10, 20])
            self.assertGreaterEqual(pipeline.queue_bytes(root), 15)

if __name__ == '__main__':
    unittest.main()

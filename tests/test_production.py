import csv
import io
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch
from PIL import Image
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
import production


class ProductionTest(unittest.TestCase):
    def test_row_preserved_and_failed_image_missing(self):
        image = io.BytesIO()
        Image.new('RGB', (2, 2)).save(image, format='PNG')
        row = {'ticket_id': 'T', 'photo': 'https://example.test/1', 'photo_after': 'https://example.test/2', 'comment': 'ไทย', 'source_file': 'bangkok_test.csv'}
        def fake(item, timeout, attempts):
            return ({**item, 'status': 'failed', 'error': 'not found'}, None) if item['field'] == 'photo_after' else ({**item, 'status': 'ok'}, image.getvalue())
        with patch('production.download', side_effect=fake):
            result, failures = production.make_batch([row], 2, 15, 3, 100000)
        self.assertEqual(result[0]['comment'], 'ไทย')
        self.assertIsNotNone(result[0]['image_before'])
        self.assertIsNone(result[0]['image_after'])
        self.assertEqual(failures[0]['ticket_id'], 'T')
        self.assertEqual(failures[0]['field'], 'photo_after')

    def test_oversized_batch_splits_without_losing_rows(self):
        original = list(range(8))
        def fake(batch, *args):
            if len(batch) > 2:
                raise production.BatchTooLarge('too big')
            return batch, []
        with patch('production.make_batch', side_effect=fake):
            chosen, rest, output, failures = production.fit_batch(original, iter([8, 9]), 1, 15, 3, 100)
        self.assertEqual(chosen, [0, 1])
        self.assertEqual(output, [0, 1])
        self.assertEqual(list(rest), list(range(2, 10)))
        self.assertEqual(failures, [])

    def test_source_schema_validation(self):
        with tempfile.TemporaryDirectory() as d:
            path = Path(d) / 'bangkok_test.csv'
            path.write_text('ticket_id,photo\nT,url\n')
            with self.assertRaises(ValueError):
                production.inspect_sources(d)


if __name__ == '__main__':
    unittest.main()

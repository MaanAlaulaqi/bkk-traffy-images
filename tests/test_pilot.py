import csv
import importlib.util
import io
import json
from pathlib import Path
import tarfile
import tempfile
import unittest

spec = importlib.util.spec_from_file_location('pilot', Path(__file__).resolve().parents[1] / 'src/pilot.py')
pilot = importlib.util.module_from_spec(spec)
spec.loader.exec_module(pilot)


class PilotTest(unittest.TestCase):
    def test_sampling_and_manifest_preserve_associations(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'bangkok_2024-01.csv'
            with path.open('w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=['ticket_id', 'photo', 'photo_after'])
                writer.writeheader()
                writer.writerow({'ticket_id': 'T1', 'photo': 'https://example.test/a', 'photo_after': 'https://example.test/b'})
            sample, population = pilot.sample_links(tmp, 5, 1)
            self.assertEqual(population, 2)
            self.assertEqual({x['field'] for x in sample}, {'photo', 'photo_after'})
            archive = Path(tmp) / 'a.tar'
            pilot.build_archive(archive, [({**sample[0], 'status': 'ok', 'content_type': 'image/jpeg'}, b'image'), ({**sample[1], 'status': 'failed'}, None)])
            with tarfile.open(archive) as tar:
                manifest = json.load(tar.extractfile('manifest.json'))
                self.assertEqual(manifest[0]['ticket_id'], 'T1')
                self.assertEqual(tar.extractfile(manifest[0]['archive_path']).read(), b'image')
                self.assertNotIn('archive_path', manifest[1])


if __name__ == '__main__':
    unittest.main()

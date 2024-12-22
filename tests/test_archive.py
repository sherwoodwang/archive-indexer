import base64
import hashlib
import tempfile
import unittest
from pathlib import Path

from arindexer import Archive
# noinspection PyProtectedMember
from arindexer._processor import Processor


class ArchiveTest(unittest.TestCase):
    def test_simple_rebuild(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            archive_path = Path(tmpdir) / 'test_archive'
            archive_path.mkdir()

            seed = bytes.fromhex('5e628956f09045be6321c389af2d7405fafc18a0d7f44e8727a886f7fb5b5beb')

            def generate(path, salt: str):
                encoded_salt = salt.encode('utf-8')
                segment = seed

                with open(path, 'wb') as f:
                    for _ in range(128):
                        segment = bytes(hashlib.sha256(encoded_salt + segment + encoded_salt).digest())
                        f.write(segment)

            generate(archive_path / 'sample0', 'sample0')
            (archive_path / 'sample1').mkdir()
            generate(archive_path / 'sample1' / 'sample2', 'sample2')
            generate(archive_path / 'sample1' / 'sample3', 'sample3')
            (archive_path / 'sample1' / 'sample4').mkdir()
            generate(archive_path / 'sample1' / 'sample4' / 'sample5', 'sample5')
            (archive_path / 'sample6').mkdir()
            (archive_path / 'sample6' / 'sample7').mkdir()
            (archive_path / 'sample8').mkdir()
            for i in range(9, 9 + 64):
                generate(archive_path / 'sample8' / f'sample{i}', f'sample{i}')

            with Processor() as processor:
                with Archive(processor, str(archive_path)) as archive:
                    archive.rebuild()
                    archive.rebuild()

                target = Path(tmpdir) / 'target'
                target_filtered = Path(tmpdir) / 'targets_filtered'

                target.mkdir()
                generate(target / 'sample-a', 'sample5')
                generate(target / 'sample-b', 'sample72')
                generate(target / 'retained', 'retained')

                with Archive(processor, str(archive_path)) as archive:
                    archive.filter(target, target_filtered)

                    self.assertTrue((target_filtered / 'retained').exists())
                    self.assertFalse((target_filtered / 'sample-a').exists())
                    self.assertFalse((target_filtered / 'sample-b').exists())


if __name__ == '__main__':
    unittest.main()

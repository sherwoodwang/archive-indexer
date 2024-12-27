import hashlib
import re
import tempfile
import unittest
from pathlib import Path

from arindexer import Archive, FileMetadataDifferencePattern, FileDifferenceKind, Output
# noinspection PyProtectedMember
from arindexer._processor import Processor


async def compute_xor(path: Path):
    data = path.read_bytes()
    value = 0
    while data:
        if len(data) > 4:
            seg = data[:4]
            data = data[4:]
        else:
            seg = (data + b'\0\0\0\0')[:4]
            data = b''

        value = value ^ int.from_bytes(seg)

    value = int.to_bytes(value, length=4)

    return value


class ArchiveTest(unittest.TestCase):
    def test_rebuild_and_filter(self):
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
            (archive_path / 'sample74').mkdir()
            for i in range(9, 9 + 16):
                generate(archive_path / 'sample74' / f'sample{i}-another', f'sample{i}')

            with Processor() as processor:
                with Archive(processor, str(archive_path)) as archive:
                    archive.rebuild()
                    archive.rebuild()
                    self.assertEqual(
                        set(archive.inspect()),
                        {
                            'config hash-algorithm sha256',
                            'file-hash 012e06abd2e40aebd85e416c4c84d0409e131e7831b10fae1944972d01c03753 0 sample8/sample41',
                            'file-hash 0576fefc966dc91b9860dc21a525cbbf4999330d3bd193973ca7e67624a4951b 0 sample8/sample38',
                            'file-hash 11aa7f108a573d417055076df566d7aa9eebc0cdadf1a56c5a8fe863ba9c9215 0 sample8/sample26',
                            'file-hash 12a8fbcf449740b0f29b2fbfb3a230bba37a4e6646edfbcf5ee496e3890654db 0 sample8/sample25',
                            'file-hash 135becd8bed65d802d364bb78de13346a224b868f916cf94890abf98132be6cf 0 sample8/sample63',
                            'file-hash 136cdcb759165287d147fec152647e978863d4e4afa690a92edade60b45767d0 0 sample74/sample12-another sample8/sample12',
                            'file-hash 1a80c910748e958a17c224d3b81964651fa21a39aa5c8c35349be236347e9f5c 0 sample8/sample46',
                            'file-hash 2214d29b396a8a90ed00bc2f3eaa256bbafad4e1c6c435a854abd6292b07c5d4 0 sample74/sample19-another sample8/sample19',
                            'file-hash 223d7c8fd01523f372e59d37a2f2687412517a2e6e1de056ef7b0b8d72983bda 0 sample74/sample17-another sample8/sample17',
                            'file-hash 238263920e4a3f8d16b059e579f5cc230c460002a489a8b72f4d153c3f09b37c 0 sample74/sample18-another sample8/sample18',
                            'file-hash 2449ceaa12a2aa69d6ddddd8fbdc0082e1f2502bfe2f0981902be110f6cdee9a 0 sample8/sample34',
                            'file-hash 2474eaf85cb0639cde7fe627d2ac1b4c35d2954bea741cd27467fd0b09c08ee8 0 sample74/sample24-another sample8/sample24',
                            'file-hash 257eaf26e2d6a2d32df4071cb13b03fda30fae73c43f687c67002e9712e51dea 0 sample74/sample20-another sample8/sample20',
                            'file-hash 2634e9330f01fe4c58dc6cc662950d5052cfcfaad548a4fd628abdf5606173c0 0 sample1/sample3',
                            'file-hash 26bf32952a0dc59435259194b9323291d76a42260f659324a628816d3dace8be 0 sample8/sample39',
                            'file-hash 28762fc981421f49914f83d787db9b01eae2a8164f354ad26040d1ec2e0a4ab7 0 sample8/sample28',
                            'file-hash 29b86ae2615ab49d97dba25f4ffc5cc978dc0e3d0f430249fee899bf1288ae55 0 sample8/sample67',
                            'file-hash 2afa535b548d86aa2a4df31d9888f39781673d96f96858cb2d828e4ed2e7f9d0 0 sample8/sample36',
                            'file-hash 3295e3883b6f050e81f0eb6e8adb918ffab48d462c607cc748e4e71378ee64e6 0 sample74/sample10-another sample8/sample10',
                            'file-hash 3d3a9471ba773371c2e8bc0062902998ae9670c6ebdffddef099dacff689947e 0 sample8/sample32',
                            'file-hash 3fa58d89f4bd3da260e82c97c52797da02ae2023e0212af8d0002dd50cfb05de 0 sample8/sample30',
                            'file-hash 404d2f903a37f5ced38fffeb3de448042e6ef3d53b68e3cccc2e0dc22ce3345f 0 sample8/sample50',
                            'file-hash 42bd2f28d3981fba5c5724643295a1e260573252d225091854ca29e3961bb264 0 sample74/sample15-another sample8/sample15',
                            'file-hash 497eb0f9148be123fb0da47cdcbbd295059967a25f1cd7ea689cba6e2032d92a 0 sample8/sample68',
                            'file-hash 4b11d98c1abf0c21a2bf580dd9ac93465b5b758597c4234f0fcf44c3aa2ac3d6 0 sample8/sample58',
                            'file-hash 4cf0fcd52aab31f22e81c6be7b2f1af2debb2fe54c5d79e58b215abcb51f7ffc 0 sample8/sample35',
                            'file-hash 4d14b93abe681be5ddbd1b91f5a0baf90e78ee1915650d2b0b1133d12c287259 0 sample8/sample60',
                            'file-hash 4e9b494860bf0965e6747e6afecc83d533ec8beb5ab2afbf3f5c22c53813e673 0 sample8/sample55',
                            'file-hash 534313de331e22512357096fb352920c9ece6b95da8b04463616c67b2a2bd6a2 0 sample8/sample71',
                            'file-hash 5689b392afa5dacae25f8bf28911ee15cc0286674f97b29cc21b35798f5065ea 0 sample8/sample53',
                            'file-hash 58f702416181867426ada5024387ed8a9550045c7dad5e91e93f2e29d0d00580 0 sample8/sample64',
                            'file-hash 5eb822a2efa053972594a90f391f5f4a034c3012a480d25757dd4517e1d26a84 0 sample74/sample11-another sample8/sample11',
                            'file-hash 6e29bcf2e47b64a1e2d627f713c19346ab66f65d21224bdad3c42e39bde14fa2 0 sample1/sample4/sample5',
                            'file-hash 6f5053cc306f2c5b6e628ad81f1dffe22dd0f96fb7b458e61fdc1e9a7288d10d 0 sample8/sample56',
                            'file-hash 72a5d130758b490c4ef1028880497f7aed35f023656078bdf1089a9f4dd47305 0 sample8/sample49',
                            'file-hash 77a38ff151a30571a4e7714cc769ab6f5f6625a8794ec4e1e964d8ba0db2b2a7 0 sample8/sample37',
                            'file-hash 7d9fd3d23b4667e38bd42a423a05bf8f9ccca0c02de4ef0c33362172b75e1bb7 0 sample8/sample61',
                            'file-hash 7f4ae9a602f9465922d963522a0ab6769d0e5cc82fc673f57c60acf989d1c932 0 sample8/sample72',
                            'file-hash 82905bea0acc3cb31743bf33742400f012853b2e594bf54223de8215dc78903f 0 sample8/sample48',
                            'file-hash 86706b83c230caa30452914e83c5195eac86843533ff431da5c79f9d6f50f6c8 0 sample8/sample29',
                            'file-hash 87e4e3ffe9ba611988963fd46650556d4614a87052eb1a242a7a9af862a8f895 0 sample8/sample43',
                            'file-hash 892fb0fe890cae54f9b2ee6d69b878a4e4bfa92dc347d8a3247b7354cbf13c1d 0 sample8/sample65',
                            'file-hash 8977f673a6362d5449dda5624436760da8eff0f3795e2695a7b6ebdd8d0c7e2d 0 sample8/sample47',
                            'file-hash 89f7ec3b7a7f896f51437f4c4e4495c5375e9c9d67fd136884a7da1a2ed4fff2 0 sample8/sample27',
                            'file-hash 8e69352112d0bfa715150620ab4e8795d7a573d4c3d37704a6a05898d7eac19f 0 sample8/sample42',
                            'file-hash 91982a1e448df468100989f8a636da81db548bd172e818a48fbf1835c8aa28ed 0 sample8/sample33',
                            'file-hash 98c4b5a8000f6a8d5b78a50bb63b12db94713fe11d7ffffd16109823dfe4ea5b 0 sample8/sample40',
                            'file-hash 9cddc2186f98808b3ea1499a2f8ece90924d24ef2c209c3c639178805c1dfd3d 0 sample8/sample54',
                            'file-hash a17a6e8362432e2ee7861e5dea9ee2d4f0db4a8cb4d8f13db2cb91d22cc05849 0 sample8/sample44',
                            'file-hash a22a64ba859507f8507c67b59fea9464c3a9221a821d66f19e61573ef7764e2c 0 sample8/sample57',
                            'file-hash b6a978dc68d17edb4dfe1a32607b1caa1cc87a48b8b37195cb256beb2b4bc908 0 sample8/sample59',
                            'file-hash b9eca1d9632e955e1eb8aeb5bee3626033832877dfa8e5c72e35f0c3d112dbff 0 sample8/sample45',
                            'file-hash c0739ec6913c9d1cf3488822bc80f858821e824f71882f0d30ed0f59b0e837dc 0 sample8/sample66',
                            'file-hash c1a8534d37e8e43e575340a502e8a636d5ea162776a68b355f2aaa37981377a3 0 sample8/sample70',
                            'file-hash c2d30048b140845806f6b2b6842d2c2e19508493c5a83e34525f6feedaddd5c5 0 sample74/sample16-another sample8/sample16',
                            'file-hash d1bf55ac9bfda1cd87bbed609cdd471d41cfee58d9545db9a46f0333f3b36be8 0 sample8/sample69',
                            'file-hash d7b1cbc3589403c892895dd1e6a71bf3e39af2e52d64ff47013e994e4822b8ca 0 sample8/sample52',
                            'file-hash d8989c5d4110956a0867b4d6732219c7ce9688a1c57aa941ea1410f7aa47d6d0 0 sample0',
                            'file-hash da548001ce90cae64be87093dd476b345e7176f2ba2bab1d446481feb27b4fc6 0 sample74/sample22-another sample8/sample22',
                            'file-hash dac70bc79724cd282f182ab6aa09847b4ecc2ba82e12060ee078994a996f56f2 0 sample8/sample51',
                            'file-hash de98d295d4d4e0cb52d67ea13f5120f2d6324e51c6594761313ab80cb2171a12 0 sample74/sample9-another sample8/sample9',
                            'file-hash df21dfc9357179eb37e510bafee9172b332d19df560c7c2170ffad8fdb9707c6 0 sample8/sample31',
                            'file-hash e41c79dfcf75f793a69cc3e294356baa09481dd7ee3e08da9e4584cdaaa859ed 0 sample74/sample13-another sample8/sample13',
                            'file-hash ef99729801a2a675fba190fe8036f7fbac4ceed2e6d6f3880af370ef76b1610f 0 sample74/sample14-another sample8/sample14',
                            'file-hash f4125996c2c17e2002149bcfc3b580e7a2e5e1d6ca6a9e173e4517b78ad6cc56 0 sample1/sample2',
                            'file-hash f72073aa85c5e441ade232bc7123ce1a220062bfa1e2227f9d285139a8344163 0 sample74/sample23-another sample8/sample23',
                            'file-hash fa2ff5ac7b6ebe8c10c5bac5a0c84f59cead8405f1e5e6006f026fcdb7976209 0 sample8/sample62',
                            'file-hash fff29af91fad946f0cca38161a5081880386d14091a405a3d96dc66522e44f78 0 sample74/sample21-another sample8/sample21',
                        }
                    )

                target = Path(tmpdir) / 'target'

                target.mkdir()
                generate(target / 'sample-a', 'sample5')
                generate(target / 'sample-b', 'sample72')
                generate(target / 'retained', 'retained')

                class CollectingOutput(Output):
                    def __init__(self):
                        super().__init__()
                        self.data: list[list[str]] = []

                    def _produce(self, record):
                        self.data.append(record)

                output = CollectingOutput()
                with Archive(processor, str(archive_path), output=output) as archive:
                    diffptn = FileMetadataDifferencePattern()
                    diffptn.ignore(FileDifferenceKind.ATIME)
                    diffptn.ignore(FileDifferenceKind.CTIME)
                    diffptn.ignore(FileDifferenceKind.MTIME)
                    archive.find_duplicates(target, ignore=diffptn)
                    self.assertEqual(set((tuple(r) for r in output.data)), {('sample-a',), ('sample-b',)})

                    output.data.clear()
                    output.verbosity = 1
                    archive.find_duplicates(target, ignore=diffptn)
                    self.assertEqual(
                        set((tuple((re.sub('^(##[^:]*):.*', '\\1', p) for p in r)) for r in output.data)),
                        {('sample-a',
                          '## identical file',
                          '## ignored difference - atime',
                          '## ignored difference - ctime',
                          '## ignored difference - mtime'),
                         ('sample-b',
                          '## identical file',
                          '## ignored difference - atime',
                          '## ignored difference - ctime',
                          '## ignored difference - mtime')}
                    )

                    output.data.clear()
                    output.verbosity = 1
                    archive.find_duplicates(target)
                    self.assertEqual(output.data, [])

                    output.data.clear()
                    output.verbosity = 2
                    archive.find_duplicates(target)
                    self.assertEqual(
                        set((tuple((re.sub('^(##[^:]*):.*', '\\1', p) for p in r)) for r in output.data)),
                        {('# possible duplicate: sample-a',
                          '## file with identical content',
                          '## difference - atime',
                          '## difference - ctime',
                          '## difference - mtime'),
                         ('# possible duplicate: sample-b',
                          '## file with identical content',
                          '## difference - atime',
                          '## difference - ctime',
                          '## difference - mtime')}
                    )

    def test_rebuild_with_collision(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            archive_path = Path(tmpdir) / 'test_archive'
            archive_path.mkdir()

            (archive_path / 'sample0').write_bytes(b'\0\0\0\0\1\1\1\1')
            (archive_path / 'sample1').write_bytes(b'\1\1\1\1\2\2\2\2')
            for i in range(2, 300):
                (archive_path / f'sample{i}').write_bytes(
                    (i * 2).to_bytes(length=2) * 2 + (i * 2 + 1).to_bytes(length=2) * 2)

            with Processor() as processor:
                with Archive(processor, str(archive_path)) as archive:
                    archive._hash_algorithms['xor'] = (4, compute_xor)
                    archive._default_hash_algorithm = 'xor'

                    archive.rebuild()

                    self.assertEqual(
                        set((rec for rec in archive.inspect())),
                        {
                            'config hash-algorithm xor',
                            'file-hash 00010001 0 sample130',
                            'file-hash 00010001 1 sample190',
                            'file-hash 00010001 256 sample75',
                            'file-hash 00010001 257 sample109',
                            'file-hash 00010001 258 sample49',
                            'file-hash 00010001 259 sample3',
                            'file-hash 00010001 260 sample32',
                            'file-hash 00010001 261 sample47',
                            'file-hash 00010001 262 sample129',
                            'file-hash 00010001 263 sample36',
                            'file-hash 00010001 264 sample285',
                            'file-hash 00010001 265 sample112',
                            'file-hash 00010001 266 sample145',
                            'file-hash 00010001 267 sample151',
                            'file-hash 00010001 268 sample74',
                            'file-hash 00010001 269 sample238',
                            'file-hash 00010001 270 sample191',
                            'file-hash 00010001 271 sample182',
                            'file-hash 00010001 272 sample16',
                            'file-hash 00010001 273 sample110',
                            'file-hash 00010001 274 sample131',
                            'file-hash 00010001 275 sample185',
                            'file-hash 00010001 276 sample50',
                            'file-hash 00010001 277 sample193',
                            'file-hash 00010001 278 sample95',
                            'file-hash 00010001 279 sample8',
                            'file-hash 00010001 280 sample150',
                            'file-hash 00010001 281 sample142',
                            'file-hash 00010001 282 sample23',
                            'file-hash 00010001 283 sample232',
                            'file-hash 00010001 284 sample89',
                            'file-hash 00010001 285 sample155',
                            'file-hash 00010001 286 sample263',
                            'file-hash 00010001 287 sample104',
                            'file-hash 00010001 288 sample254',
                            'file-hash 00010001 289 sample292',
                            'file-hash 00010001 290 sample253',
                            'file-hash 00010001 291 sample48',
                            'file-hash 00010001 292 sample106',
                            'file-hash 00010001 293 sample45',
                            'file-hash 00010001 294 sample120',
                            'file-hash 00010001 295 sample115',
                            'file-hash 00010001 296 sample143',
                            'file-hash 00010001 297 sample246',
                            'file-hash 00010001 2 sample137',
                            'file-hash 00010001 3 sample63',
                            'file-hash 00010001 4 sample111',
                            'file-hash 00010001 5 sample231',
                            'file-hash 00010001 6 sample258',
                            'file-hash 00010001 7 sample98',
                            'file-hash 00010001 8 sample153',
                            'file-hash 00010001 9 sample187',
                            'file-hash 00010001 10 sample247',
                            'file-hash 00010001 11 sample38',
                            'file-hash 00010001 12 sample161',
                            'file-hash 00010001 13 sample297',
                            'file-hash 00010001 14 sample260',
                            'file-hash 00010001 15 sample208',
                            'file-hash 00010001 16 sample60',
                            'file-hash 00010001 17 sample139',
                            'file-hash 00010001 18 sample207',
                            'file-hash 00010001 19 sample195',
                            'file-hash 00010001 20 sample90',
                            'file-hash 00010001 21 sample279',
                            'file-hash 00010001 22 sample205',
                            'file-hash 00010001 23 sample229',
                            'file-hash 00010001 24 sample225',
                            'file-hash 00010001 25 sample81',
                            'file-hash 00010001 26 sample114',
                            'file-hash 00010001 27 sample83',
                            'file-hash 00010001 28 sample92',
                            'file-hash 00010001 29 sample164',
                            'file-hash 00010001 30 sample122',
                            'file-hash 00010001 31 sample21',
                            'file-hash 00010001 32 sample234',
                            'file-hash 00010001 33 sample176',
                            'file-hash 00010001 34 sample273',
                            'file-hash 00010001 35 sample11',
                            'file-hash 00010001 36 sample119',
                            'file-hash 00010001 37 sample256',
                            'file-hash 00010001 38 sample138',
                            'file-hash 00010001 39 sample123',
                            'file-hash 00010001 40 sample206',
                            'file-hash 00010001 41 sample24',
                            'file-hash 00010001 42 sample189',
                            'file-hash 00010001 43 sample233',
                            'file-hash 00010001 44 sample28',
                            'file-hash 00010001 45 sample236',
                            'file-hash 00010001 46 sample213',
                            'file-hash 00010001 47 sample93',
                            'file-hash 00010001 48 sample4',
                            'file-hash 00010001 49 sample265',
                            'file-hash 00010001 50 sample126',
                            'file-hash 00010001 51 sample55',
                            'file-hash 00010001 52 sample269',
                            'file-hash 00010001 53 sample168',
                            'file-hash 00010001 54 sample125',
                            'file-hash 00010001 55 sample278',
                            'file-hash 00010001 56 sample237',
                            'file-hash 00010001 57 sample134',
                            'file-hash 00010001 58 sample61',
                            'file-hash 00010001 59 sample282',
                            'file-hash 00010001 60 sample262',
                            'file-hash 00010001 61 sample15',
                            'file-hash 00010001 62 sample154',
                            'file-hash 00010001 63 sample298',
                            'file-hash 00010001 64 sample25',
                            'file-hash 00010001 65 sample2',
                            'file-hash 00010001 66 sample85',
                            'file-hash 00010001 67 sample108',
                            'file-hash 00010001 68 sample103',
                            'file-hash 00010001 69 sample276',
                            'file-hash 00010001 70 sample73',
                            'file-hash 00010001 71 sample34',
                            'file-hash 00010001 72 sample255',
                            'file-hash 00010001 73 sample118',
                            'file-hash 00010001 74 sample26',
                            'file-hash 00010001 75 sample228',
                            'file-hash 00010001 76 sample200',
                            'file-hash 00010001 77 sample144',
                            'file-hash 00010001 78 sample100',
                            'file-hash 00010001 79 sample299',
                            'file-hash 00010001 80 sample68',
                            'file-hash 00010001 81 sample132',
                            'file-hash 00010001 82 sample12',
                            'file-hash 00010001 83 sample166',
                            'file-hash 00010001 84 sample127',
                            'file-hash 00010001 85 sample196',
                            'file-hash 00010001 86 sample180',
                            'file-hash 00010001 87 sample53',
                            'file-hash 00010001 88 sample135',
                            'file-hash 00010001 89 sample209',
                            'file-hash 00010001 90 sample214',
                            'file-hash 00010001 91 sample147',
                            'file-hash 00010001 92 sample257',
                            'file-hash 00010001 93 sample65',
                            'file-hash 00010001 94 sample221',
                            'file-hash 00010001 95 sample172',
                            'file-hash 00010001 96 sample181',
                            'file-hash 00010001 97 sample160',
                            'file-hash 00010001 98 sample272',
                            'file-hash 00010001 99 sample7',
                            'file-hash 00010001 100 sample220',
                            'file-hash 00010001 101 sample78',
                            'file-hash 00010001 102 sample290',
                            'file-hash 00010001 103 sample116',
                            'file-hash 00010001 104 sample250',
                            'file-hash 00010001 105 sample107',
                            'file-hash 00010001 106 sample264',
                            'file-hash 00010001 107 sample175',
                            'file-hash 00010001 108 sample67',
                            'file-hash 00010001 109 sample64',
                            'file-hash 00010001 110 sample18',
                            'file-hash 00010001 111 sample30',
                            'file-hash 00010001 112 sample82',
                            'file-hash 00010001 113 sample197',
                            'file-hash 00010001 114 sample288',
                            'file-hash 00010001 115 sample51',
                            'file-hash 00010001 116 sample286',
                            'file-hash 00010001 117 sample194',
                            'file-hash 00010001 118 sample294',
                            'file-hash 00010001 119 sample13',
                            'file-hash 00010001 120 sample252',
                            'file-hash 00010001 121 sample184',
                            'file-hash 00010001 122 sample71',
                            'file-hash 00010001 123 sample198',
                            'file-hash 00010001 124 sample179',
                            'file-hash 00010001 125 sample29',
                            'file-hash 00010001 126 sample54',
                            'file-hash 00010001 127 sample157',
                            'file-hash 00010001 128 sample43',
                            'file-hash 00010001 129 sample183',
                            'file-hash 00010001 130 sample40',
                            'file-hash 00010001 131 sample17',
                            'file-hash 00010001 132 sample167',
                            'file-hash 00010001 133 sample128',
                            'file-hash 00010001 134 sample124',
                            'file-hash 00010001 135 sample287',
                            'file-hash 00010001 136 sample192',
                            'file-hash 00010001 137 sample5',
                            'file-hash 00010001 138 sample86',
                            'file-hash 00010001 139 sample162',
                            'file-hash 00010001 140 sample59',
                            'file-hash 00010001 141 sample222',
                            'file-hash 00010001 142 sample226',
                            'file-hash 00010001 143 sample159',
                            'file-hash 00010001 144 sample261',
                            'file-hash 00010001 145 sample56',
                            'file-hash 00010001 146 sample77',
                            'file-hash 00010001 147 sample31',
                            'file-hash 00010001 148 sample289',
                            'file-hash 00010001 149 sample219',
                            'file-hash 00010001 150 sample173',
                            'file-hash 00010001 151 sample35',
                            'file-hash 00010001 152 sample243',
                            'file-hash 00010001 153 sample171',
                            'file-hash 00010001 154 sample156',
                            'file-hash 00010001 155 sample14',
                            'file-hash 00010001 156 sample188',
                            'file-hash 00010001 157 sample210',
                            'file-hash 00010001 158 sample105',
                            'file-hash 00010001 159 sample146',
                            'file-hash 00010001 160 sample271',
                            'file-hash 00010001 161 sample39',
                            'file-hash 00010001 162 sample102',
                            'file-hash 00010001 163 sample44',
                            'file-hash 00010001 164 sample216',
                            'file-hash 00010001 165 sample249',
                            'file-hash 00010001 166 sample117',
                            'file-hash 00010001 167 sample266',
                            'file-hash 00010001 168 sample87',
                            'file-hash 00010001 169 sample37',
                            'file-hash 00010001 170 sample174',
                            'file-hash 00010001 171 sample66',
                            'file-hash 00010001 172 sample133',
                            'file-hash 00010001 173 sample212',
                            'file-hash 00010001 174 sample244',
                            'file-hash 00010001 175 sample97',
                            'file-hash 00010001 176 sample20',
                            'file-hash 00010001 177 sample268',
                            'file-hash 00010001 178 sample72',
                            'file-hash 00010001 179 sample170',
                            'file-hash 00010001 180 sample42',
                            'file-hash 00010001 181 sample121',
                            'file-hash 00010001 182 sample140',
                            'file-hash 00010001 183 sample148',
                            'file-hash 00010001 184 sample217',
                            'file-hash 00010001 185 sample224',
                            'file-hash 00010001 186 sample277',
                            'file-hash 00010001 187 sample149',
                            'file-hash 00010001 188 sample94',
                            'file-hash 00010001 189 sample274',
                            'file-hash 00010001 190 sample158',
                            'file-hash 00010001 191 sample113',
                            'file-hash 00010001 192 sample46',
                            'file-hash 00010001 193 sample186',
                            'file-hash 00010001 194 sample245',
                            'file-hash 00010001 195 sample215',
                            'file-hash 00010001 196 sample242',
                            'file-hash 00010001 197 sample295',
                            'file-hash 00010001 198 sample84',
                            'file-hash 00010001 199 sample235',
                            'file-hash 00010001 200 sample69',
                            'file-hash 00010001 201 sample293',
                            'file-hash 00010001 202 sample70',
                            'file-hash 00010001 203 sample79',
                            'file-hash 00010001 204 sample211',
                            'file-hash 00010001 205 sample88',
                            'file-hash 00010001 206 sample10',
                            'file-hash 00010001 207 sample6',
                            'file-hash 00010001 208 sample241',
                            'file-hash 00010001 209 sample204',
                            'file-hash 00010001 210 sample165',
                            'file-hash 00010001 211 sample33',
                            'file-hash 00010001 212 sample99',
                            'file-hash 00010001 213 sample178',
                            'file-hash 00010001 214 sample96',
                            'file-hash 00010001 215 sample281',
                            'file-hash 00010001 216 sample62',
                            'file-hash 00010001 217 sample227',
                            'file-hash 00010001 218 sample283',
                            'file-hash 00010001 219 sample218',
                            'file-hash 00010001 220 sample267',
                            'file-hash 00010001 221 sample27',
                            'file-hash 00010001 222 sample136',
                            'file-hash 00010001 223 sample270',
                            'file-hash 00010001 224 sample275',
                            'file-hash 00010001 225 sample248',
                            'file-hash 00010001 226 sample19',
                            'file-hash 00010001 227 sample291',
                            'file-hash 00010001 228 sample251',
                            'file-hash 00010001 229 sample203',
                            'file-hash 00010001 230 sample152',
                            'file-hash 00010001 231 sample169',
                            'file-hash 00010001 232 sample223',
                            'file-hash 00010001 233 sample296',
                            'file-hash 00010001 234 sample259',
                            'file-hash 00010001 235 sample76',
                            'file-hash 00010001 236 sample230',
                            'file-hash 00010001 237 sample101',
                            'file-hash 00010001 238 sample202',
                            'file-hash 00010001 239 sample280',
                            'file-hash 00010001 240 sample41',
                            'file-hash 00010001 241 sample80',
                            'file-hash 00010001 242 sample9',
                            'file-hash 00010001 243 sample284',
                            'file-hash 00010001 244 sample52',
                            'file-hash 00010001 245 sample177',
                            'file-hash 00010001 246 sample199',
                            'file-hash 00010001 247 sample57',
                            'file-hash 00010001 248 sample141',
                            'file-hash 00010001 249 sample91',
                            'file-hash 00010001 250 sample58',
                            'file-hash 00010001 251 sample239',
                            'file-hash 00010001 252 sample240',
                            'file-hash 00010001 253 sample163',
                            'file-hash 00010001 254 sample22',
                            'file-hash 00010001 255 sample201',
                            'file-hash 01010101 0 sample0',
                            'file-hash 03030303 0 sample1',
                        }
                    )


if __name__ == '__main__':
    unittest.main()

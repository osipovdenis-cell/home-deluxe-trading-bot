"""Decrypt a downloaded report locally. The private key must never enter GitHub."""
import argparse
import json
import os
from pathlib import Path
import subprocess
import tempfile
import time
import zipfile


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('artifact', type=Path, help='Downloaded ZIP or CMS .p7m')
    parser.add_argument('--key', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    if zipfile.is_zipfile(args.artifact):
        with zipfile.ZipFile(args.artifact) as archive:
            matches = [item for item in archive.infolist() if item.filename == 'latest.p7m']
            if len(matches) != 1 or matches[0].file_size > 20_000_000:
                parser.error('Expected one bounded latest.p7m ciphertext in the ZIP')
            encrypted = archive.read(matches[0])
    else:
        encrypted = args.artifact.read_bytes()
    result = subprocess.run(
        ['openssl', 'cms', '-decrypt', '-binary', '-inform', 'DER', '-inkey', str(args.key)],
        input=encrypted, capture_output=True, timeout=30,
    )
    if result.returncode:
        parser.error('Decryption/authentication failed; nothing written')
    bundle = json.loads(result.stdout)
    args.output.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(dir=args.output.parent, delete=False) as stream:
            temporary = Path(stream.name)
            os.fchmod(stream.fileno(), 0o600)
            stream.write(result.stdout)
        os.replace(temporary, args.output)
    finally:
        if temporary:
            temporary.unlink(missing_ok=True)
    age = time.time() - float(bundle['generated_at_unix'])
    print(f"Report generated {bundle['generated_at']}; age {age / 60:.1f} min")
    if age > 1800:
        print('STALE: snapshot is older than 30 minutes; do not treat it as current.')


if __name__ == '__main__':
    main()

"""Glue Python-shell job: shared Google Drive folder -> S3 Raw.

The Drive folder is shared as "Anyone with the link" (public CMS data), so no
Google account, key or secret is needed. The folder listing and downloads use
the gdown library.

Only files not seen before are downloaded. A manifest at
s3://<RAW_BUCKET>/_manifest/manifest.json records each loaded Drive file id with
its size and md5. A file replaced in place under the SAME Drive id is not
detected; run the job with --FORCE_REFRESH true to re-download everything.
Files are routed by name: PBJ_* -> pbj/, everything else -> cms/.
"""
import hashlib
import json
import os
import shutil
import sys
import tempfile

# Glue's home directory is not writable, and gdown keeps a cookie file under ~/.cache
os.environ["HOME"] = tempfile.mkdtemp()

import boto3
import gdown
from awsglue.utils import getResolvedOptions

REQUIRED = ["RAW_BUCKET", "DRIVE_FOLDER_ID"]
args = getResolvedOptions(sys.argv, REQUIRED + (["FORCE_REFRESH"] if "--FORCE_REFRESH" in sys.argv else []))
FORCE = args.get("FORCE_REFRESH", "false").lower() == "true"
s3 = boto3.client("s3")
BUCKET, MANIFEST_KEY = args["RAW_BUCKET"], "_manifest/manifest.json"


def load_manifest():
    try:
        return json.loads(s3.get_object(Bucket=BUCKET, Key=MANIFEST_KEY)["Body"].read())
    except s3.exceptions.NoSuchKey:
        return {}


def md5_of(path):
    h = hashlib.md5()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(8 * 1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def main():
    manifest = load_manifest()
    url = f"https://drive.google.com/drive/folders/{args['DRIVE_FOLDER_ID']}"
    # list only (no download); requires the folder to be shared by link
    listing = gdown.download_folder(url=url, skip_download=True, quiet=True, remaining_ok=True)
    if not listing:
        raise RuntimeError("No files listed. Is the Drive folder shared as 'Anyone with the link'?")
    csvs = [f for f in listing if f.path.lower().endswith(".csv")]
    print(f"{len(csvs)} csv files in Drive folder")

    loaded = skipped = 0
    work = tempfile.mkdtemp()
    for f in csvs:
        name = os.path.basename(f.path)
        if f.id in manifest and not FORCE:
            skipped += 1
            continue
        local = os.path.join(work, name)
        out = gdown.download(id=f.id, output=local, quiet=True)
        if not out or not os.path.exists(local) or os.path.getsize(local) == 0:
            raise RuntimeError(f"download failed for {name} (Google may be throttling; retry later)")
        digest, size = md5_of(local), os.path.getsize(local)
        prefix = "pbj" if name.upper().startswith("PBJ_") else "cms"
        key = f"{prefix}/{name}"
        s3.upload_file(local, BUCKET, key)
        os.unlink(local)
        manifest[f.id] = {"name": name, "key": key, "bytes": size, "md5": digest}
        loaded += 1
        print(f"loaded {name} -> s3://{BUCKET}/{key}")
        # save after every file so a failure never repeats finished files
        s3.put_object(Bucket=BUCKET, Key=MANIFEST_KEY, Body=json.dumps(manifest).encode())
    shutil.rmtree(work, ignore_errors=True)
    print(f"done: {loaded} loaded, {skipped} already loaded")


main()

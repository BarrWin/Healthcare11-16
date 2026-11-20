"""Glue Python-shell job: Google Drive -> S3 Raw.

Downloads only files that are new or changed since the last run, tracked in a
manifest at s3://<RAW_BUCKET>/_manifest/manifest.json (Drive md5 + modifiedTime).
Drive credentials (service-account JSON) come from AWS Secrets Manager.
Files are routed by name: PBJ_* -> pbj/, everything else -> cms/.
"""
import io
import json
import os
import sys
import tempfile

import boto3
from awsglue.utils import getResolvedOptions
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

args = getResolvedOptions(sys.argv, ["RAW_BUCKET", "SECRET_NAME", "DRIVE_FOLDER_ID"])
s3 = boto3.client("s3")
BUCKET, MANIFEST_KEY = args["RAW_BUCKET"], "_manifest/manifest.json"


def load_manifest():
    try:
        return json.loads(s3.get_object(Bucket=BUCKET, Key=MANIFEST_KEY)["Body"].read())
    except s3.exceptions.NoSuchKey:
        return {}


def drive_client():
    secret = boto3.client("secretsmanager").get_secret_value(SecretId=args["SECRET_NAME"])
    creds = service_account.Credentials.from_service_account_info(
        json.loads(secret["SecretString"]),
        scopes=["https://www.googleapis.com/auth/drive.readonly"])
    return build("drive", "v3", credentials=creds, cache_discovery=False)


def walk(drive, folder_id):
    """Yield every CSV under the folder (recurses into subfolders)."""
    token = None
    while True:
        resp = drive.files().list(
            q=f"'{folder_id}' in parents and trashed=false",
            fields="nextPageToken, files(id,name,mimeType,md5Checksum,modifiedTime,size)",
            pageSize=200, pageToken=token, supportsAllDrives=True,
            includeItemsFromAllDrives=True).execute()
        for f in resp["files"]:
            if f["mimeType"] == "application/vnd.google-apps.folder":
                yield from walk(drive, f["id"])
            elif f["name"].lower().endswith(".csv"):
                yield f
        token = resp.get("nextPageToken")
        if not token:
            return


def main():
    manifest, drive = load_manifest(), drive_client()
    loaded = skipped = 0
    for f in walk(drive, args["DRIVE_FOLDER_ID"]):
        sig = {"md5": f.get("md5Checksum"), "modified": f["modifiedTime"]}
        if manifest.get(f["id"], {}).get("sig") == sig:
            skipped += 1
            continue
        prefix = "pbj" if f["name"].upper().startswith("PBJ_") else "cms"
        key = f"{prefix}/{f['name']}"
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            dl = MediaIoBaseDownload(tmp, drive.files().get_media(
                fileId=f["id"], supportsAllDrives=True), chunksize=64 * 1024 * 1024)
            done = False
            while not done:
                _, done = dl.next_chunk()
        s3.upload_file(tmp.name, BUCKET, key)
        os.unlink(tmp.name)
        manifest[f["id"]] = {"name": f["name"], "key": key, "sig": sig}
        loaded += 1
        print(f"loaded {f['name']} -> s3://{BUCKET}/{key}")
        # save after each file so a failure never re-downloads finished files
        s3.put_object(Bucket=BUCKET, Key=MANIFEST_KEY, Body=json.dumps(manifest).encode())
    print(f"done: {loaded} loaded, {skipped} unchanged")


main()

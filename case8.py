import os
import pysftp
import re
from azure.storage.blob import BlobServiceClient
from furl import furl
from paramiko.sftp_file import SFTPFile
from azure.storage.blob import ContainerClient

SFTP_URL = os.getenv("SFTP_URL")
SFTP_TRAFFIC_USERNAME = os.getenv("SFTP_TRAFFIC_USERNAME")
SFTP_TRAFFIC_PASSWORD = os.getenv("SFTP_TRAFFIC_PASSWORD")
TRAFFICDATA_STORAGE_ACCOUNT_SAS_TOKEN = os.getenv("TRAFFICDATA_STORAGE_ACCOUNT_SAS_TOKEN")
TRAFFICDATA_CONTAINER_URL = os.getenv("TRAFFICDATA_CONTAINER_URL")
patternstoload = re.compile(
    r"(\d{6}_ILLUM_CONCAT_VIABK_(FINAL|PRELIM)_SURFACE.txt.gz)|(\d{6}_UNADJ_ITIN_(CUR|ADV).txt.gz)"
)


class SftpReader:
    def __init__(self):
        self.sftp_connection = None
        self.create_sftp_connection()

    def create_sftp_connection(self):
        cnopts = pysftp.CnOpts()
        cnopts.hostkeys = None
        self.sftp_connection = pysftp.Connection(
            host=SFTP_URL, port=22, username=SFTP_TRAFFIC_USERNAME, password=SFTP_TRAFFIC_PASSWORD, cnopts=cnopts
        )

    def read_sftp_file(self, filename) -> SFTPFile:
        return self.sftp_connection.open("./" + filename, mode="r", bufsize=-1)


class BlobUploader:
    @staticmethod
    def create_blob_storage_container_client() -> ContainerClient:
        container_url = furl(TRAFFICDATA_CONTAINER_URL)
        container_name = container_url.path.segments[0]

        blob_service_client = BlobServiceClient(container_url.origin, credential=TRAFFICDATA_STORAGE_ACCOUNT_SAS_TOKEN)
        return blob_service_client.get_container_client(container=container_name)

    def upload_file_to_blob(self, filename, data):
        container = self.create_blob_storage_container_client()
        container.upload_blob(filename, data=data, overwrite=True)


def main():
    sftp = SftpReader()
    files_to_copy = sftp.sftp_connection.listdir_attr(".")
    blob = BlobUploader()
    for file in files_to_copy:
        filename = file.filename
        if not patternstoload.match(filename):
            continue
        if not file.st_size > 0:
            continue
        data = sftp.read_sftp_file(filename)
        blob.upload_file_to_blob(filename, data)


if __name__ == "__main__":
    main()  # pragma: no cover

import logging
import os
import threading
from base64 import b64decode
from datetime import datetime, timedelta

from funcy import cached_property, wrap_prop

from dvc.path_info import CloudURLInfo
from dvc.progress import Tqdm
from dvc.scheme import Schemes

from .base import BaseTree

logger = logging.getLogger(__name__)


class AzureTree(BaseTree):
    scheme = Schemes.AZURE
    PATH_CLS = CloudURLInfo
    REQUIRES = {
        "azure-storage-blob": "azure.storage.blob",
        "knack": "knack",
    }
    PARAM_CHECKSUM = "md5"
    COPY_POLL_SECONDS = 5
    LIST_OBJECT_PAGE_SIZE = 5000

    def __init__(self, repo, config):
        super().__init__(repo, config)

        url = config.get("url", "azure://")
        self.path_info = self.PATH_CLS(url)

        if not self.path_info.bucket:
            container = self._az_config.get("storage", "container_name", None)
            self.path_info = self.PATH_CLS(f"azure://{container}")

        self._conn_kwargs = {
            opt: config.get(opt) or self._az_config.get("storage", opt, None)
            for opt in ["connection_string", "sas_token"]
        }
        self._conn_kwargs["account_name"] = self._az_config.get(
            "storage", "account", None
        )
        self._conn_kwargs["account_key"] = self._az_config.get(
            "storage", "key", None
        )

    @cached_property
    def _az_config(self):
        # NOTE: ideally we would've used get_default_cli().config from
        # azure.cli.core, but azure-cli-core has a lot of conflicts with other
        # dependencies. So instead we are just use knack directly
        from knack.config import CLIConfig

        config_dir = os.getenv(
            "AZURE_CONFIG_DIR", os.path.expanduser(os.path.join("~", ".azure"))
        )
        return CLIConfig(config_dir=config_dir, config_env_var_prefix="AZURE")

    def isdir(self, path_info):
        return self._get_md5sum_file(path_info.bucket, path_info.path) is None

    @wrap_prop(threading.Lock())
    @cached_property
    def blob_service(self):
        # pylint: disable=no-name-in-module
        from azure.storage.blob import BlockBlobService
        from azure.common import AzureMissingResourceHttpError

        logger.debug(f"URL {self.path_info}")
        logger.debug(f"Connection options {self._conn_kwargs}")
        blob_service = BlockBlobService(**self._conn_kwargs)
        logger.debug(f"Container name {self.path_info.bucket}")
        try:  # verify that container exists
            blob_service.list_blobs(
                self.path_info.bucket, delimiter="/", num_results=1
            )
        except AzureMissingResourceHttpError:
            blob_service.create_container(self.path_info.bucket)
        return blob_service

    def _get_md5sum_file(self, bucket, path):
        file_md5 = self.blob_service.get_blob_properties(
            bucket, path
        ).properties.content_settings.content_md5

        return b64decode(file_md5.strip('"')).hex() if file_md5 else None

    def _generate_download_url(self, path_info, expires=3600):
        from azure.storage.blob import (  # pylint:disable=no-name-in-module
            BlobPermissions,
        )

        expires_at = datetime.utcnow() + timedelta(seconds=expires)

        sas_token = self.blob_service.generate_blob_shared_access_signature(
            path_info.bucket,
            path_info.path,
            permission=BlobPermissions.READ,
            expiry=expires_at,
        )
        download_url = self.blob_service.make_blob_url(
            path_info.bucket, path_info.path, sas_token=sas_token
        )
        return download_url

    def exists(self, path_info, use_dvcignore=True):
        return self.blob_service.exists(path_info.bucket, path_info.path)

    def _list_paths(self, bucket, prefix):
        blob_service = self.blob_service
        next_marker = None
        while True:
            blobs = blob_service.list_blobs(
                bucket, prefix=prefix, marker=next_marker
            )

            for blob in blobs:
                yield blob.name

            if not blobs.next_marker:
                break

            next_marker = blobs.next_marker

    def walk_files(self, path_info, **kwargs):
        if not kwargs.pop("prefix", False):
            path_info = path_info / ""

        for fname in self._list_paths(
            path_info.bucket, path_info.path, **kwargs
        ):
            if fname.endswith("/"):
                continue

            if (
                self._get_md5sum_file(bucket=path_info.bucket, path=fname)
                is None
            ):
                continue

            yield path_info.replace(path=fname)

    def remove(self, path_info):
        if path_info.scheme != self.scheme:
            raise NotImplementedError

        logger.debug(f"Removing {path_info}")
        self.blob_service.delete_blob(path_info.bucket, path_info.path)

    def get_file_hash(self, path_info):
        return self._get_md5sum_file(path_info.bucket, path_info.path)

    def _upload(
        self, from_file, to_info, name=None, no_progress_bar=False, **_kwargs
    ):
        with Tqdm(desc=name, disable=no_progress_bar, bytes=True) as pbar:
            self.blob_service.create_blob_from_path(
                to_info.bucket,
                to_info.path,
                from_file,
                progress_callback=pbar.update_to,
            )

    def _download(
        self, from_info, to_file, name=None, no_progress_bar=False, **_kwargs
    ):
        with Tqdm(desc=name, disable=no_progress_bar, bytes=True) as pbar:
            self.blob_service.get_blob_to_path(
                from_info.bucket,
                from_info.path,
                to_file,
                progress_callback=pbar.update_to,
            )

    def copy(self, from_info, to_info):
        source_url = self.blob_service.make_blob_url(
            from_info.bucket, from_info.path
        )
        self.blob_service.copy_blob(
            container_name=to_info.bucket,
            blob_name=to_info.path,
            copy_source=source_url,
        )

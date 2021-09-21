import threading
from urllib.parse import urlparse, urlunparse

from funcy import cached_property, wrap_prop

from dvc.hash_info import HashInfo
from dvc.path_info import CloudURLInfo
from dvc.scheme import Schemes

# pylint:disable=abstract-method
from .fsspec_wrapper import CallbackMixin, FSSpecWrapper


class WebHDFSFileSystem(CallbackMixin, FSSpecWrapper):
    scheme = Schemes.WEBHDFS
    PATH_CLS = CloudURLInfo
    REQUIRES = {"fsspec": "fsspec"}
    PARAM_CHECKSUM = "checksum"

    def _with_bucket(self, path):
        if isinstance(path, self.PATH_CLS):
            return f"/{path.path.rstrip('/')}"
        return path

    @staticmethod
    def _get_kwargs_from_urls(urlpath):
        from fsspec.implementations.webhdfs import WebHDFS

        return (
            WebHDFS._get_kwargs_from_urls(  # pylint:disable=protected-access
                urlpath
            )
        )

    def _parse_config(self, path=None, alias=None):
        import configparser

        from hdfs.config import Config
        from hdfs.util import HdfsError

        try:
            config = Config(path)
        except HdfsError:
            return None

        if alias is None:
            try:
                alias = config.get(config.global_section, "default.alias")
            except configparser.Error:
                return None

        if config.has_section(alias):
            return dict(config.items(alias))

    def _prepare_credentials(self, **config):
        hdfs_config = self._parse_config(
            path=config.pop("hdfscli_config", None),
            alias=config.pop("webhdfs_alias", None),
        )
        if hdfs_config is not None:
            # If this is not None, then we will solely use
            # config options from this.

            if "url" in hdfs_config:
                url = urlparse(hdfs_config.pop("url"))
                hdfs_config["use_ssl"] = url.scheme == "https"
                webhdfs_url = urlunparse(url._replace(scheme="webhdfs"))

                # override URL options (such as user) with the explicitly
                # given ones
                hdfs_config = {
                    **self._get_kwargs_from_urls(webhdfs_url),
                    **hdfs_config,
                }

            if "root" in hdfs_config:
                raise ValueError(
                    '"root" option is not supported any '
                    "more for webhdfs remotes."
                )

            return hdfs_config

        if "webhdfs_token" in config:
            config["token"] = config.pop("webhdfs_token")

        return config

    @wrap_prop(threading.Lock())
    @cached_property
    def fs(self):
        from fsspec.implementations.webhdfs import WebHDFS

        return WebHDFS(**self.fs_args)

    def checksum(self, path_info):
        path = self._with_bucket(path_info)
        ukey = self.fs.ukey(path)

        return HashInfo(
            "checksum", ukey["bytes"], size=self.getsize(path_info)
        )

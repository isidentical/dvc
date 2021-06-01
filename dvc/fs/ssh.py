import getpass
import os.path
import threading
from urllib.parse import urlparse

from funcy import cached_property, first, memoize, silent, wrap_prop, wrap_with

import dvc.prompt as prompt
from dvc.scheme import Schemes

from .fsspec_wrapper import FSSpecWrapper

_SSH_TIMEOUT = 60 * 30
_SSH_CONFIG_FILE = os.path.expanduser(os.path.join("~", ".ssh", "config"))


@wrap_with(threading.Lock())
@memoize
def ask_password(host, user, port):
    return prompt.password(
        "Enter a private key passphrase or a password for "
        "host '{host}' port '{port}' user '{user}'".format(
            host=host, port=port, user=user
        )
    )


def _load_user_ssh_config(hostname):
    return {}


# pylint:disable=abstract-method
class SSHFileSystem(FSSpecWrapper):
    scheme = Schemes.SSH
    REQUIRES = {"sshfs": "sshfs"}

    DEFAULT_PORT = 22
    PARAM_CHECKSUM = "md5"

    def _prepare_credentials(self, **config):
        login_info = {}

        url = config.get("url")
        assert url

        parts = urlparse(url)
        user_ssh_config = _load_user_ssh_config(parts.hostname)

        host = user_ssh_config.get("hostname", parts.hostname)
        user = (
            config.get("user")
            or parts.username
            or user_ssh_config.get("user")
            or getpass.getuser()
        )
        port = (
            config.get("port")
            or parts.port
            or silent(int)(user_ssh_config.get("port"))
            or self.DEFAULT_PORT
        )
        path_info = self.PATH_CLS.from_parts(
            scheme=self.scheme,
            host=host,
            user=user,
            port=port,
            path=parts.path,
        )
        self.path_info = path_info

        if user_ssh_config.get("identityfile"):
            config.setdefault(
                "keyfile", first(user_ssh_config["identityfile"])
            )

        login_info["host"] = path_info.host
        login_info["username"] = path_info.user
        login_info["port"] = path_info.port
        login_info["client_keys"] = [config.get("keyfile")]

        login_info["timeout"] = config.get("timeout", _SSH_TIMEOUT)
        if config.get("password"):
            login_info["password"] = config["password"]

        # aes128-gcm is much faster than the default encryption
        # algorithm that comes with asyncssh, so we'll use it.
        login_info["encryption_algs"] = "aes128-gcm@openssh.com"

        # see: https://github.com/ronf/asyncssh/issues/374
        login_info["compression_algs"] = None

        # login_info["gss_auth"] = config.get("gss_auth", False)
        # login_info["allow_agent"] = config.get("allow_agent", True)
        # proxy_command = user_ssh_config.get("proxycommand")
        # if proxy_command:
        #    import paramiko
        #
        #    login_info["sock"] = paramiko.ProxyCommand(proxy_command)

        # NOTE: we use the same password regardless of the server :(
        # if config.get("ask_password") and login_info["password"] is None:
        #     host, user, port = path_info.host, path_info.user, path_info.port
        #     login_info["password"] = ask_password(host, user, port)

        login_info["known_hosts"] = None

        return login_info

    @wrap_prop(threading.Lock())
    @cached_property
    def fs(self):
        from sshfs import SSHFileSystem

        return SSHFileSystem(**self.fs_args)

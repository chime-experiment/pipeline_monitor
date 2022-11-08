import paramiko
import sys
import subprocess
import logging
from typing import Tuple

logger = logging.getLogger(__name__)


class CommandClient:
    """Client for executing bash commands."""

    @classmethod
    def from_config(cls, config):
        if config.get("remote"):
            return _SSHAutoConnectClient.from_config(config)
        return _LocalClient.from_config(config)


class _LocalClient:
    """Client for executing command line commands with
    an optional executable/script.
    """

    _command_prefix = ""
    _exec = ""

    @classmethod
    def from_config(cls, config):
        self = cls()

        # Format prefix required for executing commands
        prefix = f"module use {config.get('modpath', '')}; "
        for m in config.get("modules", []):
            prefix += f"module load {m}; "
        prefix += f"source {config.get('venv', '')}; "

        self._command_prefix = prefix
        self._exec = config.get("exec", "")

        return self

    def exec_get_result(self, command: str):

        command = self._command_prefix + command

        stdout, stderr = subprocess.Popen(
            [self._exec, command], stdout=subprocess.PIPE, stderr=subprocess.PIPE
        ).communicate()

        return stdout, stderr


class _SSHAutoConnectClient(paramiko.SSHClient):
    """Extends paramiko.SSHClient to make some tasks more
    automatic.

    Parameters
    ----------
    login : dict
        ssh login info. must include host and user
    encoding : str
        encoding to expect from output
    private : bool
        whether or not to load ssh keys
    """

    _command_prefix = ""

    def __init__(self, login: dict, encoding: str, private: bool = False):
        super().__init__()
        self._encoding = encoding
        # Loads all system keys. This should be improved.
        if private:
            self.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            self.load_system_host_keys()
        # Establish ssh connection
        self.connect(
            hostname=login.get("ssh_hostname", ""),
            username=login.get("ssh_username", ""),
            password=login.get("ssh_password", ""),
            key_filename=login.get("ssh_key_filename", ""),
        )

    @classmethod
    def from_config(cls, config: dict) -> "_SSHAutoConnectClient":
        """Create a class instance from an ssh config dict.

        Parameters
        ----------
        config : dict
            dictionary with ssh connection parameters

        Returns
        -------
        SSHAutoConnect : class
            class instance initialized by config file
        """
        login_keys = [
            "ssh_hostname",
            "ssh_username",
            "ssh_password",
            "ssh_key_filename",
        ]
        login = {k: config.get(k, "") for k in login_keys}
        # Default to system encoding if nothing provided
        self = cls(
            login=login,
            encoding=config.get("ssh_encoding", sys.stdout.encoding),
            private=config.get("ssh_private", False),
        )

        # Format prefix required for executing commands
        prefix = f"module use {config.get('modpath', '')}; "
        for m in config.get("modules", []):
            prefix += f"module load {m}; "
        prefix += f"source {config.get('venv', '')}; "

        self._command_prefix = prefix

        return self

    def exec_get_result(self, command: str) -> Tuple[str, str]:
        """Execute a command over ssh and automatically
        read the result/output.

        Parameters
        ----------
        command : str
            bash command to execute

        Returns
        -------
        result : str
            stdout return from command
        error : str
            stderr return from command
        """
        command = self._command_prefix + command

        (_, stdout, stderr) = self.exec_command(command)
        result = stdout.read().decode(self._encoding)
        error = stderr.read().decode(self._encoding)

        return result, error

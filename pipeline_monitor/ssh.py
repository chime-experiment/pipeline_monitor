import paramiko
import sys
from typing import Tuple


class SSHAutoConnect(paramiko.SSHClient):
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

    _venv = ""

    def __init__(self, login: dict, encoding: str, private: bool = False):
        super().__init__()
        self._encoding = encoding
        # Loads all system keys. This should be improved.

        if private:
            self.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            self.load_system_host_keys()
        # Establish ssh connection
        self.connect(
            hostname=login.get("hostname", ""),
            username=login.get("username", ""),
            password=login.get("password", ""),
            key_filename=login.get("key_filename", "")
        )

    @classmethod
    def from_config(cls, config: dict) -> "SSHAutoConnect":
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
        login_keys = ["hostname", "username", "password"]
        login = {k: config.get(k, "") for k in login_keys}
        venv = config.get("venv", "")
        # Default to system encoding if nothing provided
        self = cls(
            login=login,
            encoding=config.get("encoding", sys.stdout.encoding),
            private=config.get("private", False),
        )

        if venv:
            self._venv = venv

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
        if self._venv:
            command = f"source {self._venv}; " + command
        (_, stdout, stderr) = self.exec_command(command)
        result = stdout.read().decode(self._encoding)
        error = stderr.read().decode(self._encoding)

        return result, error

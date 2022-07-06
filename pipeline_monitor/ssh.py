import paramiko
import sys
from typing import Tuple
from yaml import safe_load
from pathlib import Path


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
        )

    @classmethod
    def from_yaml_file(cls, yaml_path: str) -> "SSHAutoConnect":
        """Create a class instance from a yaml config file.

        Parameters
        ----------
        yaml_path : str
            path to yaml file

        Returns
        -------
        SSHAutoConnect : class
            class instance initialized by config file
        """
        with (Path(yaml_path)).open() as fh:
            yaml_file = safe_load(fh)

        return cls.from_yaml_str(yaml_file)

    @classmethod
    def from_yaml_dict(cls, yaml_file: dict) -> "SSHAutoConnect":
        """Create a class instance from a yaml config dict

        Parameters
        ----------
        yaml_file : dict
            loaded yaml file

        Returns
        -------
        SSHAutoConnect : class
            class instance initialized by config file
        """
        # Make sure the config contains properly formatted
        # ssh parameters
        try:
            if not isinstance(yaml_file["ssh"], dict):
                raise TypeError(
                    "Invalid ssh config format. Expected 'dict', got '{0}'".format(
                        type(yaml_file["ssh"]).__name__
                    )
                )
        except KeyError as e:
            raise KeyError("Couldn't find key 'ssh' in YAML configuration file.") from e

        return cls.from_config(yaml_file["ssh"])

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

        # Default to system encoding if nothing provided
        self = cls(
            login=login,
            encoding=config.get("encoding", sys.stdout.encoding),
            private=config.get("private", False),
        )

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
        (_, stdout, stderr) = self.exec_command(command)
        result = stdout.read().decode(self._encoding)
        error = stderr.read().decode(self._encoding)

        return result, error

import subprocess
import logging

logger = logging.getLogger(__name__)


class ScriptClient:
    """Client to run a script remotely and return the result."""

    def __init__(self, exec, exec_args=[]):
        self._exec = exec
        self.exec_args = exec_args

    def exec_script(self, script_path: str, script_args: list = []):
        """ """
        # Maybe we should resolve the path here?

        stdout, stderr = subprocess.Popen(
            [self._exec, *self.exec_args, script_path, *script_args],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        ).communicate()

        return stdout.decode(), stderr.decode()

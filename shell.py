import os
import subprocess as _sp

def shcmd(cmd, si=None, stdout=_sp.PIPE, stderr=_sp.PIPE, shell=True, cwd=None, timeout=None, env=None):
    """Execute command using subprocess and returns a tuple (cmd output, cmd stderr, result code)"""

    env_vars = {}
    env_vars.update(os.environ)
    if env is not None:
        env_vars.update(env)

    if not isinstance(cmd, list):
        cmd = [cmd]

    cmdobj = _sp.run(cmd, shell=shell, stdout=stdout, stderr=stderr, cwd=cwd, timeout=timeout, env=env_vars)

    try:
        stdout = [ln for ln in cmdobj.stdout.decode().split("\n") if len(ln)]
        stderr = [ln for ln in cmdobj.stderr.decode().split("\n") if len(ln)]
        res = (stdout, stderr, cmdobj.returncode)
        return res
    except OSError as e:
        return (None, e, cmdobj.returncode)

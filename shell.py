import subprocess as _sp


def shcmd(cmd, si=None, shell=True):
    """Execute command using subprocess and returns a tuple (cmd output, cmd stderr, result code)"""

    rawcmd = []
    if not isinstance(cmd, list):
        cmd = [cmd]

    for c in cmd:
        rawcmd.append("%s" % (c))

    cmdobj = _sp.Popen(rawcmd, stdin=si, stdout=_sp.PIPE, stderr=_sp.PIPE, shell=shell)

    try:
        (cmdout, cmderr) = cmdobj.communicate()
        cmdout = cmdout.decode().split("\r\n")
        res = (cmdout, cmderr, cmdobj.returncode)
        return res
    except OSError as e:
        return (None, e, cmdobj.returncode)

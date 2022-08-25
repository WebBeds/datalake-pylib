import re

def parse_command(
    commands: list,
    oenv: dict
) -> list:
    if not oenv:
        return commands
    for idx, command in enumerate(commands):
        r = re.compile(r'\$\{\{(.*?)\}\}')
        s = r.search(command)
        if not s:
            continue
        if s.group(1) not in oenv:
            continue
        commands[idx] = r.sub(lambda x: oenv[x.group(1)], command)
    return commands

def get_command(
    entrypoint: list,
    command: list,
    oenv: dict = None
) -> list:
    cmd = []
    if entrypoint and len(entrypoint) > 0:
        cmd.extend(entrypoint)
    if command and len(command) > 0:
        cmd.extend(command)
    return parse_command(cmd, oenv)
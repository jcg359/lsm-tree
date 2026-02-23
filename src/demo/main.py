import readline
import src.demo.controller as ctrl

args_cmd = {"load": ctrl.load, "search": ctrl.search, "delete": ctrl.delete}
single_cmd = {"input": ctrl.save_input, "truncate": ctrl.truncate, "compact": ctrl.compact}
show_cmd = {"count": ctrl.level_counts, "memtable": ctrl.memtable_keys}


while True:
    print()
    raw = input("LSM Storage - enter command or 'help': ").strip().lower()
    cmds = " ".join(raw.split()).split(" ")

    if cmds[0] == "exit":
        break

    if cmds[0] == "help":
        ctrl.help()
        continue

    if cmds[0] == "clear-readline-history":
        readline.clear_history()
        continue

    print("-")

    if cmds[0] in args_cmd:
        args_cmd[cmds[0]](cmds)
        continue

    if cmds[0] in single_cmd:
        single_cmd[cmds[0]]()
        continue

    if cmds[0] in show_cmd:
        show_cmd[cmds[0]]()
        continue

    print(f"Command {cmds[0]} not found")

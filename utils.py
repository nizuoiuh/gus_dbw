# based on https://jazeabby.medium.com/a-simple-progress-bar-in-python-knowhows-a4e70b7ca85d
def progress_bar(
    iteration: int,
    total: int,
    prefix: str = "",
    suffix: str = "",
    decimals: int = 1,
    length: int = 60,
    fill="█",
    linesUp: int = 0,
) -> None:
    percent = ("{0:." + str(decimals) + "f}").format(
        100 * (iteration / float(total)))
    filledLength = int(length * iteration // total)
    bar = fill * filledLength + "-" * (length - filledLength)
    if linesUp:
        print(f"\r\033[{linesUp}A", end="", flush=True)
    print(f"\r{prefix} |{bar}| {percent}% {suffix}", end="", flush=True)
    if linesUp:
        print(f"\r\033[{linesUp}B", end="", flush=True)

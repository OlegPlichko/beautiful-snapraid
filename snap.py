import threading
import subprocess
import logging
import logging.handlers
import sys
import time
from collections import Counter, defaultdict
import re
from functools import reduce
import os
import traceback
from functools import wraps
import time
from glob import glob


CONFIG_PATH = "/etc/snapraid.conf"
SNAPRAID_PATH = "/usr/local/bin/snapraid"
LOG_PAT = "/var/log/snap.log"
LOG_MAX_SIZE = 5000
BASE_PATH = '/mnt/Storage1-1/'
RAID_DIR = '/RAID'
FULL_PATH = BASE_PATH + RAID_DIR
delete_threshold = 100
NOT_IMPORTANT = [
    'RAID/AppData/big-bear-trilium',
    'RAID/AppData/photoprism', 
    'RAID/AppData/jellyfin', 
    'RAID/AppData/big-bear-libretranslate',
    'RAID/AppData/big-bear-minio',
]
TIME_NOT_OPTIMIZED_FUNCTION = 0.004


def timeit(func):
    @wraps(func)
    def timeit_wrapper(*args, **kwargs):
        start_time = time.perf_counter()
        result = func(*args, **kwargs)
        end_time = time.perf_counter()
        total_time = end_time - start_time
        args_to_print = []
        for arg in args:
            if not isinstance(arg, dict) and not isinstance(arg, list):
                args_to_print.append(arg)
        if total_time > TIME_NOT_OPTIMIZED_FUNCTION:
            logging.warning(f'Function {func.__name__}{args_to_print} Took {total_time:.4f} seconds')
        return result
    return timeit_wrapper


def setup_logger():
    log_format = logging.Formatter("%(asctime)s [%(levelname)-6.6s] %(message)s")
    root_logger = logging.getLogger()
    logging.OUTPUT = 15
    logging.addLevelName(logging.OUTPUT, "OUTPUT")
    logging.OUTERR = 25
    logging.addLevelName(logging.OUTERR, "OUTERR")
    root_logger.setLevel(logging.WARNING)
    console_logger = logging.StreamHandler(sys.stdout)
    console_logger.setFormatter(log_format)
    root_logger.addHandler(console_logger)

    if LOG_PAT:
        max_log_size = max(LOG_MAX_SIZE, 0) * 1024
        file_logger = logging.handlers.RotatingFileHandler(
            LOG_PAT, maxBytes=max_log_size, backupCount=9
        )
        file_logger.setFormatter(log_format)
        root_logger.addHandler(file_logger)


def tee_log(infile, out_lines, log_level):
    """
    Create a thread that saves all the output on infile to out_lines and
    logs every line with log_level
    """

    def tee_thread():
        for line in iter(infile.readline, ""):
            logging.log(log_level, line.rstrip())
            out_lines.append(line)
        infile.close()

    t = threading.Thread(target=tee_thread)
    t.daemon = True
    t.start()
    return t


def snapraid_command(command, args={}, *, allow_statuscodes=[]):
    """
    Run snapraid command
    Raises subprocess.CalledProcessError if errorlevel != 0
    """
    arguments = ["--conf", CONFIG_PATH, "--quiet"]
    for k, v in args.items():
        arguments.extend(["--" + k, str(v)])
    p = subprocess.Popen(
        [SNAPRAID_PATH, command] + arguments,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        # Snapraid always outputs utf-8 on windows. On linux, utf-8
        # also seems a sensible assumption.
        encoding="utf-8",
        errors="replace",
    )
    out = []
    threads = [
        tee_log(p.stdout, out, logging.OUTPUT),
        tee_log(p.stderr, [], logging.OUTERR),
    ]
    for t in threads:
        t.join()
    ret = p.wait()
    # sleep for a while to make pervent output mixup
    time.sleep(0.3)
    if ret == 0 or ret in allow_statuscodes:
        return out
    else:
        raise subprocess.CalledProcessError(ret, "snapraid " + command)


def main():
    open("snap.sh", "w").close() # clear file
    outfile = open("snap.sh", "w")
    try:
        run(outfile)
    except Exception as e:
        logging.error(e)
        logging.log(traceback.print_stack())
    finally:
        outfile.close()


@timeit
def get_diff(command_out):
    results = Counter(line.split(" ")[0] for line in command_out)
    results = dict(
        (x, results[x]) for x in ["add", "remove", "move", "update"]
    )
    return results

DUPS_REGEX = re.compile(r"^\s+\d*\s(.*)\s$")
DUPS_SEPARATOR = '='


@timeit
def get_dups(command_out):
    dups = defaultdict(list)
    
    for dup in command_out:
        items = dup.split(DUPS_SEPARATOR)
        if len(items) == 2:
            key = DUPS_REGEX.search(items[0])[1]
            value = items[1].strip()
            dups[key].append(value)
            dups[value].append(key)

    full_dups = dict()
    for key, values in dups.items():
        for value in values:
            full_dups[key] = list(
                filter(lambda x: x != key, set(dups[key] + dups[value]))
            )
            full_dups[value] = list(
                filter(lambda x: x != value, set(dups[value] + dups[key]))
            )

    return full_dups


@timeit
def check_if_removed_a_copy(diff_out, removed_file):
    for line in diff_out:
        linesplit = line.split(" ")
        if linesplit[0] == 'copy':
            if removed_file == linesplit[1]:
                return True
    return False


@timeit
def check_if_removed_is_hidden(removed_file):
    basename = os.path.basename(removed_file)
    if basename.startswith('.'):
        return True
    return False


@timeit
def check_has_copy_in_path(removed_file):
    name = os.path.basename(removed_file).replace('\\', '')
    removed_file_split = removed_file.split('/')
    if len(removed_file_split) > 1:
        path_dir = removed_file_split[1]
        find_in_path = f'{FULL_PATH}/{path_dir}/**/{name}'
        if glob(find_in_path, recursive=True):
            return True
    return False


@timeit
def get_removed_items_dups_important_and_not_important_and_hidden_or_copies(diff_out, dups):
    removed_not_important = defaultdict(list)
    removed_items = set([line[7:].strip() for line in diff_out if line.startswith("remove")])
    removed_dups = dict()
    removed_important = []
    hidden_or_copies = []
    is_not_important = False
    for line in removed_items:
        is_not_important = False
        for folder in NOT_IMPORTANT:
            if folder in line:
                removed_not_important[folder].append(line)
                is_not_important = True
                break
        if is_not_important:
            continue
        if line in dups.keys():
            not_removed_dups = [
                dup for dup in dups[line] if dup not in removed_items
            ]
            if len(not_removed_dups) > 0:
                removed_dups[line] = not_removed_dups
                continue
        if not check_if_removed_a_copy(diff_out, line) and not check_if_removed_is_hidden(line) and not check_has_copy_in_path(line):
            removed_important.append(line)
        else:
            hidden_or_copies.append(line)
    return removed_items, removed_dups, removed_important, removed_not_important, hidden_or_copies


@timeit
def get_paths_by_folder(paths):
    by_folder = defaultdict(list)
    for path in paths:
        folder = os.path.dirname(path)
        by_folder[folder].append(path)
    return by_folder


def get_removed_not_important_len(removed_not_important):
    if not removed_not_important:
        return 0
    try:
        return reduce(lambda x, y: len(x) + len(y), removed_not_important.values())
    except TypeError:
        length = 0
        for values in removed_not_important.values():
            if isinstance(values, list):
                length += len(values)
        return length


def write_to_file(outfile, text):
    for line in text.split('\n'):
        outfile.write(f'\necho {line}')
    outfile.write(f"\necho {'-'*10}\n")


def run(outfile):
    setup_logger()
    logging.warning("Running diff...")
    diff_out = snapraid_command("diff", allow_statuscodes=[2])
    diff_results = get_diff(diff_out)
    diff_warn = (
        "Diff results: {add} added,  {remove} removed, {move} moved,  {update} modified"
    ).format(**diff_results)
    logging.warning(diff_warn)
    write_to_file(outfile, diff_warn)

    if diff_results["remove"] > 0:
        logging.warning("Running dup...")
        dup_out = snapraid_command("dup", allow_statuscodes=[2])
        #dups_amount = dup_out[-1].strip().split(" ")[0]
        dups_warn = f"Dup results: {dup_out[-1].strip()}"
        logging.warning(dups_warn)
        write_to_file(outfile, dups_warn)
        dups = get_dups(
            dup_out
        )

        removed_items, removed_dups, removed_important, removed_not_important, hidden_or_copies = get_removed_items_dups_important_and_not_important_and_hidden_or_copies(
            diff_out, dups
        )

        hidden_or_copies_warn = "Deleted {} hidden or copies:\n{}".format(
            len(hidden_or_copies),
            '\n'.join(['- {item}' for item in hidden_or_copies])
        )
        logging.warning(hidden_or_copies_warn)
        write_to_file(outfile, hidden_or_copies_warn)
        removed_dups_len = len(removed_dups.keys())
        removed_dups_warn = f"Deleted duplicates {removed_dups_len}"
        logging.warning(removed_dups_warn)
        write_to_file(outfile, removed_dups_warn)

        if delete_threshold >= 0 and diff_results["remove"] > delete_threshold:
            #logging.info(
            #    "Deleted {} files exceed delete threshold of {}".format(
            #        diff_results["remove"], delete_threshold
            #    )
            #)
            removed_not_important_len = get_removed_not_important_len(removed_not_important)

            removed_important_not_dups = (
                diff_results["remove"] - removed_not_important_len - removed_dups_len
            )
            
            removed_important_warn = "Deleted {} important files are:\n{}".format(
                len(removed_important),
                '\n'.join(['- {item}' for item in removed_important])
            )
            logging.warning(removed_important_warn)
            write_to_file(outfile, removed_important_warn)
            if removed_important_not_dups <= delete_threshold:
                removed_dups_by_folder = get_paths_by_folder(removed_dups.keys())
                removed_dups_warn = "Deleted {} files are duplicates:\n{}".format(
                    removed_dups_len,
                    "\n".join(
                        [
                            f"- {key} {len(values)}"
                            for key, values in removed_dups_by_folder.items()
                        ]
                    ),
                )
                logging.warning(removed_dups_warn)
                write_to_file(outfile, removed_dups_warn)
                if len(removed_not_important) > 0:
                    removed_not_important_warn = "Deleted {} files are not important:\n{}".format(
                        removed_not_important_len,
                        "\n".join(
                            [
                                f"- {key} {len(values)}"
                                for key, values in removed_not_important.items()
                            ]
                        ),
                    )
                    logging.warning(removed_not_important_warn)
                    write_to_file(outfile, removed_not_important_warn)
                logging.info("Continue")
            else:
                removed_not_dups = [
                    value for value in removed_items if value not in dups
                ]
                logging.info("not in")
                logging.info(
                    "Deleted {} files arn't duplicates:\n{}".format(
                        len(removed_not_dups),
                        "\n".join([f"- {value}" for value in removed_not_dups]),
                    )
                )
                logging.info("Aborting")
                logging.error("Run again with --ignore-deletethreshold to sync anyways")
                return
    continue_warn = """
read -p "Continue (y/n)?" CONT
if [ "$CONT" = "y" ]; then
  snapraid touch;
  snapraid sync;
  snapraid scrub -p new;
else
  echo "abort the mission";
fi
"""
    outfile.write(continue_warn)


main()

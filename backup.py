#!/usr/bin/env python3

import argparse
import datetime
import subprocess
import sys
import time
import yaml

def say(s):
    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] BACKSTATUS: {s}")
    sys.stdout.flush()

def run(args, config, cmdline):
    prefix = "would run" if args.dry_run else "running"
    say(f"{prefix}: {cmdline}")

    if not args.dry_run:
        subprocess.run(
            cmdline,
            stdout=sys.stdout,
            stderr=sys.stdout,
            env={
                'BORG_PASSPHRASE': config['backup-password'],
            },
        )

def backup_one(args, config, backupspec):
    now = datetime.datetime.now()
    remote_repo =  f"{config['backup-host']}:{backupspec['remote-repo']}"
    archive_name = f"{remote_repo}::{now.strftime(config['archive-name-format'])}"

    say("\n\n")
    say(f"Starting backup to: {archive_name}")

    # Create a new backup
    cmdline = [
        "borg",
        "create",
        "--stats",
        "--exclude-caches",
    ]
    cmdline += backupspec.get('extra-args', [])
    for exclude in backupspec.get('exclude', []):
        cmdline += ["--exclude", exclude]
    cmdline += [
        archive_name,
    ]
    cmdline += backupspec['local-dirs']

    run(args, config, cmdline)

    # Prune old backups
    cmdline = [
        "borg",
        "prune",
        "--stats",
        remote_repo,
        "--keep-hourly", "50",
        "--keep-daily", "90",
        "--keep-weekly", "90",
        "--keep-monthly", "1000",
    ]
    run(args, config, cmdline)

    # List all backups
    cmdline = [
        "borg",
        "list",
        remote_repo,
    ]
    run(args, config, cmdline)

def backup(args, config):
    say("Starting up")
    now = datetime.datetime.now()
    is_workday = now.hour >= 7 and now.hour <= 23

    for backupspec in config['backup-specs']:
        # Slow backups are only done at 4am
        if is_workday and backupspec.get('is-slow', False):
            say(f"Skipping mid-day backup of {backupspec['remote-repo']}")
            continue
        backup_one(args, config, backupspec)

    say("Finishing\n\n\n")

def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--dry-run',
        action='store_true',
        default=False,
        help="Don't actually back up",
    )
    parser.add_argument(
        '-c', '--config',
        required=True,
        type=str,
        help='Backup config file (YAML)',
    )
    return parser.parse_args()

def main():
    args = get_args()

    with open(args.config, 'r') as f:
        config = yaml.safe_load(f)

    if not args.dry_run:
        sys.stdout = open(config['logfile'], 'a')
        sys.stderr = sys.stdout

    backup(args, config)

if __name__ == "__main__":
    main()

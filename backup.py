#!/usr/bin/env python3

import datetime
import subprocess
import sys
import time

def say(s):
    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] BACKSTATUS: {s}")
    sys.stdout.flush()

def run(config, cmdline):
    say(f"running: {cmdline}")
    subprocess.run(
        cmdline,
        stdout=sys.stdout,
        stderr=sys.stdout,
        env={
            'BORG_PASSPHRASE': config['backup-password'],
        },
    )

def backup_one(config, backupspec):
    remote_repo =  f"{config['backup-host']}:{backupspec['remote-repo']}"

    say("\n\n")
    say(f"Starting backup to: {remote_repo}")

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
        f"{remote_repo}::{config['date']}",
    ]
    cmdline += backupspec['local-dirs']

    run(config, cmdline)

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
    run(config, cmdline)

    # List all backups
    cmdline = [
        "borg",
        "list",
        remote_repo,
    ]
    run(config, cmdline)

def backup(config):
    say("Starting up")
    now = datetime.datetime.now()
    is_workday = now.hour >= 7 and now.hour <= 23

    for backupspec in config['backup-specs']:
        # Slow backups are only done at 4am
        if is_workday and backupspec.get('is-slow', False):
            say(f"Skipping mid-day backup of {backupspec['remote-repo']}")
            continue
        backup_one(config, backupspec)

    say("Finishing\n\n\n")

def main():
    sys.stdout = open(BACKUP_CONFIG['logfile'], 'a')
    backup(BACKUP_CONFIG)

if __name__ == "__main__":
    main()

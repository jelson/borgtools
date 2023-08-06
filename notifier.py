#!/usr/bin/env python3

from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import make_msgid
import argparse
import boto3
import html
import json
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import plotly.subplots
import subprocess
import yaml

# Email send methods
def construct_message(config, subj, body):
    msg = MIMEMultipart('mixed')
    msg['Subject'] = subj
    msg['From'] = config['email-from']
    msg['To'] = ",".join(config['email-to'])

    alternatives = MIMEMultipart('alternative')
    alternatives.attach(MIMEText(body, 'html', 'utf-8'))
    msg.attach(alternatives)

    return msg.as_string()

# Local file - used for dev/debug, just writes the HTML to the local filesystem
def email_localfile(args, config, subj, body):
    fn = 'test-email.html'
    print(f'Writing test email to {fn}')
    with open(fn, 'w') as f:
        f.write(f"<p>From: {html.escape(config['email-from'])}</p>\n")
        f.write(f"<p>To: {html.escape(','.join(config['email-to']))}</p>\n")
        f.write(f"<p>Subj: {subj}</p>\n")
        f.write(f"<p><p>{body}\n")

def email_aws(args, config, subj, body):
    msg = construct_message(config, subj, body)
    session = boto3.Session(profile_name=config['email-aws-profile'])
    client = session.client('ses', region_name = 'us-west-2')
    client.send_raw_email(
        Source=config['email-from'],
        Destinations=config['email-to'],
        RawMessage={
            'Data': msg,
        },
    )

def email_sendmail(args, config, subj, body):
    msg = construct_message(config, subj, body)
    raise Exception("unimplemented")

EMAIL_METHODS = {
    'debug': email_localfile,
    'aws': email_aws,
    'sendmail': email_sendmail,
}

#### Report generation

def get_backup_stats(args, config, archive):
    if args.debug:
        with open('borg-stats-example.json', 'r') as f:
            return json.load(f)
    else:
        cmd = [
            'borg',
            'info',
            f"{config['backup-host']}:{archive['remote-repo']}",
            '--last',
            str(config['email-num-backups']),
            '--json',
        ]
        print('running: ' + ' '.join(cmd))
        o = subprocess.check_output(
            cmd,
            text=True,
            env={
                'BORG_PASSPHRASE': config['backup-password'],
            }
        )
        return json.loads(o)

def generate_one_report(args, config, archive):
    w = []
    r = []

    j = get_backup_stats(args, config, archive)
    df = pd.json_normalize(j['archives'])
    df['Date'] = pd.to_datetime(df['start'])

    r.append('<h3>' + archive['remote-repo'] + '</h3>')

    fig = plotly.subplots.make_subplots(
        rows=2,
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.02,
    )
    fig.update_layout(
        height=400,
        margin=dict(l=50,r=50,b=0,t=10),
        showlegend=False,
    )
    fig.append_trace(
        go.Scatter(
            x=df['Date'],
            y=df['stats.original_size'],
            mode='lines+markers',
        ),
        row=1,
        col=1,
    )
    fig.append_trace(
        go.Scatter(
            x=df['Date'],
            y=df['stats.nfiles'],
            mode='lines+markers',
        ),
        row=2,
        col=1,
    )
    fig.update_yaxes(
        rangemode="tozero",
        tickformat='.3s',
    )
    fig.update_yaxes(row=1, title='Archive Size (bytes)')
    fig.update_yaxes(row=2, title='Number of Files')

    r.append(fig.to_html(full_html=False, include_plotlyjs='cdn'))

    return w, r

def generate_reports(args, config):
    warnings = []
    reports = []

    for archive in config['backup-specs']:
        w, r = generate_one_report(args, config, archive)
        warnings.extend(w)
        reports.extend(r)

    body = '\n'.join(warnings) + '\n'.join(reports)
    now = pd.Timestamp.now()
    subj = f'Backup report, {now.month_name()} {now.day}'
    efunc = EMAIL_METHODS[args.email_method]
    efunc(args, config, subj, body)

def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-c', '--config',
        required=True,
        type=str,
        help='Backup config file (YAML)',
    )
    parser.add_argument(
        '-e', '--email-method',
        choices=EMAIL_METHODS.keys(),
        default='debug',
        help='Email delivery method',
    )
    parser.add_argument(
        '-d', '--debug',
        default=False,
        action='store_true',
        help='Use synthetic inputs for faster debugging',
    )

    return parser.parse_args()

def main():
    args = get_args()
    with open(args.config, 'r') as f:
        config = yaml.safe_load(f)
    generate_reports(args, config)

if __name__ == '__main__':
    main()

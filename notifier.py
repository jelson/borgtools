#!/usr/bin/env python3

from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import make_msgid
from email import charset
import argparse
import boto3
import html
import humanize
import json
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import plotly.subplots
import subprocess
import sys
import yaml

# Email send methods

# Local file - used for dev/debug, just writes the HTML to the local filesystem
def email_localfile(args, config, msg):
    fn = 'test-email.eml'
    sys.stderr.write(f'Writing test email to {fn}\n')
    with open(fn, 'w') as f:
        f.write(msg.serialize())

def email_aws(args, config, msg):
    session = boto3.Session(profile_name=config['email-aws-profile'])
    client = session.client('ses', region_name = 'us-west-2')
    client.send_raw_email(
        Source=msg.fromaddr,
        Destinations=msg.toaddrs,
        RawMessage={
            'Data': msg.serialize(),
        },
    )

def email_sendmail(args, config, msg):
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

class MailMessage:
    def __init__(self, subj, fromaddr, toaddrs):
        self.warnings = []
        self.mainbody = []
        self.msg = MIMEMultipart('mixed')
        self.msg['Subject'] = subj
        self.fromaddr = fromaddr
        self.msg['From'] = fromaddr
        self.toaddrs = toaddrs
        self.msg['To'] = ",".join(toaddrs)

    def body(self, m):
        self.mainbody.append(m)

    def warn(self, w):
        self.warnings.append(w)

    def image(self, img):
        fn = f'img{len(self.mainbody)}'
        self.body(f'<img src="cid:{fn}">')
        att = MIMEImage(img, name=fn)

        att.add_header('Content-ID', f'<{fn}>')
        att.add_header('X-Attachment-Id', fn)
        att.add_header('Content-Disposition', 'inline', filename=fn)
        self.msg.attach(att)

    def serialize(self):
        body = ''.join(self.warnings) + ''.join(self.mainbody)
        alternatives = MIMEMultipart('alternative')

        # Construct a charset using Quoted Printables (base64 is default)
        cs = charset.Charset('utf-8')
        cs.body_encoding = charset.QP

        alternatives.attach(MIMEText(body, 'html', cs))
        self.msg.attach(alternatives)
        return self.msg.as_string()

def generate_one_report(args, config, archive, msg):
    j = get_backup_stats(args, config, archive)
    df = pd.json_normalize(j['archives'])
    df['Date'] = pd.to_datetime(df['start'])

    msg.body('<h3>' + archive['remote-repo'] + '</h3>')

    latest = df.iloc[df['Date'].idxmax()]
    age = pd.Timestamp.now() - latest['Date']

    msg.body(f"Latest backup was at {latest['Date']} (")
    msg.body(humanize.precisedelta(age, minimum_unit='hours'))
    msg.body(' ago). Backup size was ')
    msg.body(humanize.naturalsize(latest['stats.original_size']))
    msg.body(' across ')
    msg.body(humanize.naturalsize(latest['stats.nfiles']))
    msg.body(' files, compressed down to ')
    msg.body(humanize.naturalsize(latest['stats.compressed_size']))
    msg.body('.')

    if age > pd.Timedelta(days=2):
        msg.warn("<p style='font-weight: bold; color: red'>")
        msg.warn("WARNING: Backup ")
        msg.warn(archive['remote-repo'])
        msg.warn(" is ")
        msg.warn(humanize.precisedelta(age, minimum_unit='hours'))
        msg.warn(" old!</p>")


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

    #r.append(fig.to_html(full_html=False, include_plotlyjs='cdn'))
    msg.image(fig.to_image(format='png'))

def generate_reports(args, config):
    now = pd.Timestamp.now()
    msg = MailMessage(
        subj=f'Backup report, {now.month_name()} {now.day}',
        fromaddr=config['email-from'],
        toaddrs=config['email-to'],
    )

    for archive in config['backup-specs']:
        generate_one_report(args, config, archive, msg)

    efunc = EMAIL_METHODS[args.email_method]
    efunc(args, config, msg)

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

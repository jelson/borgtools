#!/usr/bin/env python3

from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email import charset
import argparse
import boto3
import humanize
import json
import pandas as pd
import plotly.graph_objects as go
import plotly.subplots
import subprocess
import sys
import yaml


def say(s):
    sys.stderr.write(str(s) + '\n')
    sys.stderr.flush()


####### Email send methods


# Local file - used for dev/debug, just writes the HTML to the local filesystem
def email_localfile(args, config, msg):
    fn = 'test-email.eml'
    sys.stderr.write(f'Writing test email to {fn}\n')
    with open(fn, 'w') as f:
        f.write(msg.serialize())


def email_aws(args, config, msg):
    session = boto3.Session(profile_name=config['email-aws-profile'])
    client = session.client('ses', region_name='us-west-2')
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

###### Report generation


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
    say('running: ' + ' '.join(cmd))
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
        self.header = []
        self.mainbody = []
        self.graphs = []
        self.msg = MIMEMultipart('mixed')
        self.msg['Subject'] = subj
        self.fromaddr = fromaddr
        self.msg['From'] = fromaddr
        self.toaddrs = toaddrs
        self.msg['To'] = ",".join(toaddrs)

    def head(self, w):
        self.header.append(w)

    def warn(self, archive, s):
        self.head("<p style='font-weight: bold; color: red'>")
        self.head(f"WARNING: {archive['remote-repo']}: {s}")
        self.head("</p>")

    def body(self, m):
        self.mainbody.append(m)

    def td(self, s, tdclass=None):
        self.body(f"<td")
        if tdclass:
            self.body(f' class="{tdclass}"')
        self.body(f">{s}</td>")

    def graph(self, g):
        self.graphs.append(g)

    def image(self, img):
        fn = f'img{len(self.graphs)}'
        self.graph(f'<img src="cid:{fn}">')
        att = MIMEImage(img, name=fn)

        att.add_header('Content-ID', f'<{fn}>')
        att.add_header('X-Attachment-Id', fn)
        att.add_header('Content-Disposition', 'inline', filename=fn)
        self.msg.attach(att)

    def serialize(self):
        body = ''.join(self.header) + ''.join(self.mainbody) + ''.join(self.graphs)
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

    # Get earliest and latest backups
    df['Date'] = pd.to_datetime(df['start'])
    earliest = df.iloc[df['Date'].idxmin()]
    latest = df.iloc[df['Date'].idxmax()]

    msg.body("<tr>")

    # Column 1: Partition
    msg.td(archive['remote-repo'].split('/')[-1])

    # Column 2: Date
    msg.td(latest['Date'])

    # Column 3: Age
    age = pd.Timestamp.now() - latest['Date']

    if age > pd.Timedelta(days=2):
        msg.warn(archive, f"{humanize.precisedelta(age, minimum_unit='hours')} old!")
        tdclass = 'bad'
    else:
        tdclass = 'good'
    msg.td(humanize.precisedelta(age, minimum_unit='hours'), tdclass=tdclass)

    # Column 4: Number of Files
    nfiles = latest['stats.nfiles']
    if nfiles < 100:
        msg.warn(archive, "Few files backed up!")
        tdclass = 'bad'
    else:
        tdclass = 'good'
    msg.td(humanize.metric(nfiles, precision=4), tdclass=tdclass)

    # Column 5: original size
    osize = latest['stats.original_size']
    if osize < 10000:
        msg.warn(archive, "Few bytes backed up!")
        tdclass = 'bad'
    else:
        tdclass = 'good'
    msg.td(humanize.naturalsize(osize, binary=True), tdclass=tdclass)

    # Column 6: Compressed size
    msg.td(humanize.naturalsize(latest['stats.compressed_size'], binary=True))

    msg.body("</tr>")

    # Check to see if there's been a big drop in number of files or bytes
    if earliest['stats.nfiles'] > 0:
        pct_drop_files = latest['stats.nfiles'] / earliest['stats.nfiles'] - 1
        if abs(pct_drop_files) > 0.2:
            msg.warn(archive, f"Big change in files backed up: {100*pct_drop_files:.1f}%!")

    if earliest['stats.original_size'] > 0:
        pct_drop_bytes = latest['stats.original_size'] / earliest['stats.original_size'] - 1
        if abs(pct_drop_bytes) > 0.2:
            msg.warn(archive, f"Big change in size backed up: {100*pct_drop_bytes:.1f}%!")

    fig = plotly.subplots.make_subplots(
        rows=2,
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.02,
    )
    fig.update_layout(
        height=400,
        margin=dict(l=50, r=50, b=0, t=10),
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
    msg.graph('<h3>' + archive['remote-repo'] + '</h3>')
    msg.image(fig.to_image(format='png'))


def generate_reports(args, config):
    now = pd.Timestamp.now()
    msg = MailMessage(
        subj=f'Backup report, {now.month_name()} {now.day}',
        fromaddr=config['email-from'],
        toaddrs=config['email-to'],
    )

    msg.head("""
<html>
<head>
<style>
table.backup {
    overflow: auto;
    border: 1px solid #dededf;
    table-layout: auto;
    border-collapse: collapse;
    border-spacing: 1px;
    text-align: left;
}

table.backup th {
    border: 1px solid #dededf;
    background-color: #eceff1;
    color: #000000;
    padding: 5px;
}

table.backup td {
    border: 1px solid #dededf;
    background-color: #ffffff;
    color: #000000;
    padding: 5px;
}

table.backup .good {
  background-color: lightgreen;
}

table.backup .bad {
  background-color: salmon;
}
</style>
</head>
<body>
""")

    msg.body('<table class="backup"><thead>')
    msg.body("<th>Partition</th>")
    msg.body("<th>Last Backup</th>")
    msg.body("<th>Backup Age</th>")
    msg.body("<th>Num Files</th>")
    msg.body("<th>Orig Size</th>")
    msg.body("<th>Backup Size</th>")
    msg.body("</thead><tbody>")

    for archive in config['backup-specs']:
        generate_one_report(args, config, archive, msg)

    msg.body("</tbody></table></body></html>")

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

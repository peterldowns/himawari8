#!/usr/bin/env python
# coding: utf-8
import glob
import os
import requests
import subprocess
import time
from datetime import datetime
from boto.s3.connection import S3Connection

valid_resolutions = [1, 2, 4, 8, 16, 20]

def get_format_args(dt, resolution):
  return {
      'resolution': resolution,
      'year': dt.strftime('%Y'),
      'month': dt.strftime('%m').strip().zfill(2),
      'day': dt.strftime('%d').strip().zfill(2),
      'hour': dt.strftime('%k').strip().zfill(2),
      'minute': str(dt.minute / 10 * 10).zfill(2),
    }

def get_url_template(dt, resolution):
  # http://himawari8.nict.go.jp/img/D531106/20d/550/2015/07/10/214000_{x}_{y}.png
  args = get_format_args(dt, resolution)
  args.update({
      'xtpl': '{x}',
      'ytpl': '{y}',
    })
  return ('http://himawari8.nict.go.jp/img/D531106'
          '/{resolution}d/550/{year}/{month}/{day}'
          '/{hour}{minute}00_{xtpl}_{ytpl}.png').format(**args)

def get_folder_path(dt, resolution):
  bucket_name = (
      '{resolution}d/{year}_{month}_{day}_{hour}_{minute}_00').format(
        **get_format_args(dt, resolution))
  absolute_path = os.path.abspath('./' + bucket_name)
  if not os.path.exists(absolute_path):
    os.makedirs(absolute_path)
  return bucket_name, absolute_path

def get_tile_urls(dt, resolution):
  if resolution not in valid_resolutions:
    raise Exception("Invalid resolution %d" % resolution)

  url_template = get_url_template(dt, resolution)
  urls = []
  for x in xrange(resolution):
    for y in xrange(resolution):
      urls.append((x, y, url_template.format(x=x, y=y)))
  return urls

def download(dt=None, resolutions=None):
  dt = dt or datetime.utcnow()
  resolutions = resolutions or valid_resolutions
  s3conn = S3Connection(os.environ['AWS_ACCESS_KEY'], os.environ['AWS_SECRET_KEY'])
  bucket = s3conn.get_bucket('downs-himawari8')
  print 'Downloading Earth @', dt
  for resolution in resolutions:
    s3_bucket_path, folder_path = get_folder_path(dt, resolution)
    print 'resolution =', resolution
    error = False
    for x, y, url in get_tile_urls(dt, resolution):
      filename = os.path.join(folder_path, 'y{y}_x{x}.png'.format(y=y, x=x))
      if os.path.isfile(filename):
        continue
      print url
      response = requests.get(url)
      if response.status_code != 200:
        print 'ERROR: response.status_code =', response.status_code
        error = True
        break
      with open(filename, 'wb') as fout:
        fout.write(response.content)
    if error:
      continue
    montage_commands = [
        ['montage', os.path.join(folder_path, '*.png'), '-geometry', '550x550', 'full.jpg'],
        ['montage', os.path.join(folder_path, '*.png'), '-geometry', '550x550', 'full.png'],
      ]
    for command in montage_commands:
      subprocess.check_call(command)
    for file_path in glob.iglob(os.path.join(folder_path, '*')):
      source_size = os.stat(file_path).st_size
      k = Key(bucket)
      k.key = os.path.join(s3_bucket_path, os.path.split(file_path)[-1])
      k.set_contents_from_filename(file_path)

if __name__ == '__main__':
  now = datetime(2015, 7, 10, 1, 51, 4, 531543)
  download(now, [20])

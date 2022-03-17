import luigi
import requests

from hashlib import sha256

from dstc8_reddit.config import RedditConfig, parse_date


def get_reference_checksum(src_url):
  *_, filename = src_url.split('/')

  if filename in RedditConfig().missing_checksums:
    return RedditConfig().missing_checksums[filename]

  checksums_url = RedditConfig().submissions_checksum_url_template if filename.startswith('RS') \
      else RedditConfig().comments_checksum_url_template

  print(f"Downloading checksums for {filename} from {checksums_url}")

  r = requests.get(checksums_url)
  if r.status_code != 200:
    raise RuntimeError(f"Couldn't get checksums from {checksums_url}, status={r.status_code}")

  checksum = None

  for line in r.content.decode('utf-8').split('\n'):
    if filename in line:
      checksum, *_ = line.split()
      break

  if not checksum:
    raise RuntimeError(f"Couldn't get checksum for {filename}")
    # print(f"Couldn't get checksum for {filename}")
    # return ""

  return checksum


def ignore_checksum(date, filetype):
  # Checksums are not available for comment files RC_2019-06.zst to RC_2021-06.zst
  # Ignore checksums for these files, if it falls within that date range
  
  dt = parse_date(date)
  prefix = 'RS' if filetype == 'submissions' else 'RC'
  is_ignored = False
  if (2019 <= dt.year <= 2021) and prefix=="RC":
    if dt.year == 2019:
      if dt.month >= 6:
        is_ignored = True
    else:
      is_ignored = True

  print(f"Ignoring checksum for {date} {filetype} {prefix}") if is_ignored else None

  return is_ignored


class DownloadRawFile(luigi.Task):
  date = luigi.Parameter()
  filetype = luigi.Parameter()
  resources = {"max_concurrent_downloads": 1}

  def output(self):
    dest_fp = RedditConfig().make_raw_filepath(self.date, self.filetype)
    return luigi.LocalTarget(dest_fp)

  def run(self):
    with self.output().temporary_path() as tmp_path:
      src_url = RedditConfig().make_source_url(self.date, self.filetype)
      is_ignore_checksum = ignore_checksum(self.date, self.filetype)
      if not is_ignore_checksum:
        ref_checksum = get_reference_checksum(src_url)

      print(f"Starting download of url: {src_url} => => file: {tmp_path}")
      r = requests.get(src_url, stream=True)
      if r.status_code != 200:
        raise RuntimeError(f"Error downloading {src_url}, status={r.status_code}")

      m = sha256()
      f = open(tmp_path, 'wb')
      for chunk in r.iter_content(chunk_size=RedditConfig().download_chunk_size):
        if chunk:
          f.write(chunk)
          m.update(chunk)
      f.close()

      checksum = m.hexdigest()

      if (checksum != ref_checksum) and not is_ignore_checksum:
        raise RuntimeError(f"Checksums don't match for {'RC' if self.filetype == 'comments' else 'RS'}_{self.date}!")
        # print(f"Checksums don't match for {'RC' if self.filetype == 'comments' else 'RS'}_{self.date}!")

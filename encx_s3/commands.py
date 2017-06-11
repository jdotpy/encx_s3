from encxlib.commands import BasePlugin
from encxlib.cli import FileLoaderInvalidPath

import boto3
import botocore

import configparser
import getpass
import io
import os

class S3Backend(BasePlugin):
    name = 's3'
    file_loaders = {
        's3://.*': {
            'loader': 'load_s3_file',
            'writer': 'write_s3_file',
        }   
    }
    commands = {
        's3:login': {
            'run': 'login',
            'help': 'Start new AWS session'
        },
        's3:test': {
            'run': 'test',
            'help': 'You dont need help'
        },
    }
    s3_protocol = 's3://'
    AWS_DIR = os.path.expanduser('~/.aws')
    AWS_CRED_FILE = os.path.join(AWS_DIR, 'credentials')
    AWS_CONF_FILE = os.path.join(AWS_DIR, 'config')

    DEFAULT_PROFILE = 'default'
    DEFAULT_REGION = 'us-east-1'

    def _parse_s3_uri(self, path):
        if not path.startswith(self.s3_protocol):
            raise FileLoaderInvalidPath()
        path = path[len(self.s3_protocol):] # Strip off prefix

        dirs = path.split('/')
        if len(dirs) < 2:
            # We require a bucket name and key at least so this must be invalid
            raise FileLoaderInvalidPath()

        bucket_name = dirs[0]
        file_key = '/'.join(dirs[1:])
        return bucket_name, file_key

    def _is_existing_file(self, bucket, key):
        try:
            obj = bucket.Object(key)
            obj.load()
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                return False
            else:
                raise
        return True
        
    def login(self, args):
        client_id = input('Client ID: ')
        client_secret = getpass.getpass('Client Secret: ')

        profile = input('Profile (empty for default): '.format(
            self.DEFAULT_PROFILE
        )) or self.DEFAULT_PROFILE

        region = input('Enter a region to use (defaults to "{}"): '.format(
            self.DEFAULT_REGION
        )) or self.DEFAULT_REGION
        
        if not os.path.exists(self.AWS_DIR):
            print('Making aws config directory {}.'.format(self.AWS_DIR))
            os.makedirs(self.AWS_DIR)

        # Set up creds file
        creds = configparser.ConfigParser()
        if os.path.exists(self.AWS_CRED_FILE):
            with open(self.AWS_CRED_FILE) as source:
                creds.read_file(source)

        if not creds.has_section(profile):
            creds.add_section(profile)
        creds.set(profile, 'aws_access_key_id', client_id)
        creds.set(profile, 'aws_secret_access_key', client_secret)
        with open(self.AWS_CRED_FILE, 'w') as output:
            creds.write(output)

        # Set up config file (for region)
        config = configparser.ConfigParser()
        if os.path.exists(self.AWS_CONF_FILE):
            with open(self.AWS_CONF_FILE) as source:
                config.read_file(source)

        if not config.has_section(profile):
            config.add_section(profile)
        config.set(profile, 'region', region)
        with open(self.AWS_CONF_FILE, 'w') as output:
            config.write(output)

    def test(self, args):
        bucket_name, file_key = self._parse_s3_uri('s3://who-took-my-bucket/foobar.txt')
        s3 = boto3.resource('s3')
        bucket = s3.Bucket(bucket_name)
        exists = self._is_existing_file(bucket, file_key)
        print('Does the file exist?', exists)

    def load_s3_file(self, path):
        bucket_name, file_key = self._parse_s3_uri(path)
        s3 = boto3.resource('s3')
        data = io.BytesIO()
        try:
            bucket = s3.Bucket(bucket_name)
            bucket.download_fileobj(file_key, data)
        except botocore.exceptions.ClientError as e:
            logging.error('S3 Fetch Error: ' + str(e))
            raise FileLoaderInvalidPath()

        data.seek(0)
        return data.read()

    def write_s3_file(self, path, data, overwrite=False):
        bucket_name, file_key = self._parse_s3_uri(path)
        s3 = boto3.resource('s3')
        bucket = s3.Bucket(bucket_name)
        if not overwrite and self._is_existing_file(bucket, file_key):
            raise FileExistsError
        # There's a race condition here, but its as good as AWS allows us to do
        # so we'll do the thing thats better than nothing
        bucket.upload_fileobj(io.BytesIO(data), file_key)

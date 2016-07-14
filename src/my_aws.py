'''
Class of helper functions to interact with AWS S3 services
'''

import os
import pandas as pd
from s3fs.core import S3FileSystem


class S3():
    '''
    AWS S3 class of helper functions to get and put data into S3
    '''

    def __init__(self):
        '''
        Get AWS login credentials and instantiate s3fs
        '''
        os.environ['AWS_CONFIG_FILE'] = 'aws_config.ini'
        self.s3fs = S3FileSystem(anon=False)

    def put_data(self, dataframe, key, bucket):
        '''
        Put data provided (dataframe) into S3 storage

        Args:
            dataframe (pd.dataframe): Dataframe of data to be placed in S3
            key (str): S3 bucket key that data will be saved under
            bucket (str): S3 bucket where data will be placed
        Returns:
            none
        '''
        write_bytes = dataframe.to_csv(None).encode()
        s3fs = self.s3fs
        with s3fs.open('s3://{0}/{1}'.format(bucket, key), 'wb') as file_write:
            file_write.write(write_bytes)

    def get_data(self, key, bucket):
        '''
        Get data from S3 storage

        Args:
            key (str): Data filename to pull down from S3
            bucket (str): S3 bucket where data file (key) resides
        Returns:
            pd.datframe: Pandas datframe of data
        '''
        s3_data = pd.read_csv(self.s3fs.open('{0}/{1}'.format(bucket, key),
                                             mode='rb'), index_col=0)

        return s3_data


if __name__ == '__main__':
    S3 = S3()

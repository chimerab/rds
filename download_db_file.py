import time
import boto3
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s|%(name)s|%(levelname)s|%(message)s')
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

TIME_PERIOD = 3600 * 1000 * 1 - 10 * 1000 # last one hour, minus 10 seconds just in case we skip some file. 
def get_file_list(client, dbname: str, filename_contain: str, last_writen) -> bool: 
    """get lastest file list from given database.

    :param client: rds client
    :param dbname: db instance name
    :param filename_contain: filename string should contain
    :param last_writen: only get file newer than last_written timestamp
    :return: True if file list  was fetched, else False
    """
    file_list = None
    try:
        response = client.describe_db_log_files(
            DBInstanceIdentifier=dbname,
            FilenameContains=filename_contain,
            FileLastWritten=last_writen,
            FileSize=1,
            MaxRecords=500
            #Marker='string'
        )
    except Exception as e:
        logging.info(e)
    
    file_list = response['DescribeDBLogFiles']

    is_continue = True if 'Marker' in response else False
    while is_continue is True:
        try:
            response = client.describe_db_log_files(
                DBInstanceIdentifier=dbname,
                FilenameContains=filename_contain,
                FileLastWritten=last_writen,
                FileSize=1,
                MaxRecords=500,
                Marker=response['Marker']
            )
        except Exception as e:
            logging.info(e)

        file_list += response['DescribeDBLogFiles']
        is_continue = True if 'Marker' in response else False

    return file_list

def download_file(client, dbname: str, file_in: str, file_out: str) -> bool:
    """download file from given database.

    :param client: rds client
    :param dbname: db instance name
    :param file_in: filename in db instance
    :param file_out: filename saved to local disk
    :return: True if file  was downloaded, else False
    """
    try:
        response = client.download_db_log_file_portion(
                DBInstanceIdentifier=dbname,
                LogFileName=file_in,
                Marker='0',
                NumberOfLines=5000
                )
    except Exception as e:
        logging.info(e)
        return False

    with open(file_out,'wt+') as fp:

        try:
            ret = fp.write(response['LogFileData'])

            logging.debug('file bytes writen:{}'.format(ret))
            while response['AdditionalDataPending']:
                response = client.download_db_log_file_portion(
                    DBInstanceIdentifier=dbname,
                    LogFileName=file_in,
                    Marker=response['Marker'],
                    NumberOfLines=5000
                    )   
                ret = fp.write(response['LogFileData'])       

                logging.debug('file bytes writen:{}'.format(ret))
        except Exception as e:
            logging.info(e)
            return False

    return True

def upload_file(client, file_name, bucket, object_name=None):
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = file_name

    # Upload the file
    try:
        response = client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True

def main():

    now = lambda: int(time.time()*1000)

    client = boto3.client('rds')
    file_list = get_file_list(client, 'lab', 'audit', now() - TIME_PERIOD)
    
    logging.info(f'We have {len(file_list)} files need be download.')

    for target in file_list:
        logging.info(f'Downloading file:{target}')
        ret = download_file(client,'lab',target['LogFileName'], target['LogFileName'].split('/')[1])
        if ret is False:
            logging.info('unable to download file: {}'.format(target))


if __name__ == '__main__':
    main()


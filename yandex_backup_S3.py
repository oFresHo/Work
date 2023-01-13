import boto3
import os
from datetime import datetime
import sys
import threading

class ProgressPercentage(object):

    def __init__(self, filename):
        self._filename = filename
        self._size = float(os.path.getsize(filename))
        self._seen_so_far = 0
        self._lock = threading.Lock()

    def __call__(self, bytes_amount):
        # To simplify, assume this is hooked up to a single filename
        with self._lock:
            self._seen_so_far += bytes_amount
            percentage = (self._seen_so_far / self._size) * 100
            sys.stdout.write(
                "\r%s  %s / %s  (%.2f%%)" % (
                    self._filename, self._seen_so_far, self._size,
                    percentage))
            sys.stdout.flush()


session = boto3.session.Session(profile_name='Yandex')
s3 = session.client(
    service_name='s3',
    endpoint_url='https://storage.yandexcloud.net'
)

print('Start ' + str(datetime.now()) + '\n')

# Смотрим на наличае файлов в облаке
bucketaws = []
for key in s3.list_objects(Bucket='alsi.backup-sql')['Contents']:
    bucketaws.append(key['Key'])
    # print(key['Key'])

# Получаем список файлов
content = os.listdir(os.getcwd())
cwd = os.getcwd()
cwdsplit = os.getcwd().split("\\")
print ("Workdir - " + cwd)
files_full = len(content)

# Делаем проверку на файлы с такими же именами в облаке и убираем их из загрузки
for file_bucket in bucketaws:
    for file_content in content:
        if file_content in file_bucket:
            content.remove(file_content)
# print(content)

files_check = len(content)
print ("Найдено совпадающих файлов - \"" + str(files_full - files_check) + "\" они загруженны не будут")

# Делаем выгрузку
for file in content:
    print(str(content.index(file) + 1) + '/' + str(len(content)))
    s3.upload_file('{}'.format(file), 'alsi.backup-sql', cwdsplit[-1] + '/{}'.format(file), Callback=ProgressPercentage('{}'.format(file)))
    print('\n\\{}'.format(file) + " - done\n")

print('End ' + str(datetime.now()))

os.system("pause")


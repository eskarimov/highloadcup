import zipfile, ujson, os
from multiprocessing import Pool


entities = ['users', 'locations', 'visits']
data_folder = 'data'

def write_data(entity, data_file_name):
    datazip =  zipfile.ZipFile('/tmp/data/data.zip')
    data_file = datazip.open(data_file_name)
    data = ujson.loads(data_file.read())[entity]
    output = open(os.path.join(os.path.dirname(os.path.abspath(__file__)), data_folder, data_file_name), 'w',
                  encoding='utf-8')
    output.write(ujson.dumps(data, output))

if __name__ == '__main__':
    pool = Pool()
    for entity in entities:
        datazip =  zipfile.ZipFile('/tmp/data/data.zip')
        files = datazip.namelist()
        data_files = [file for file in files if file.startswith(entity)]
        pool.starmap(write_data, [(entity, data_files)])
    pool.close()
    print('DATA UNZIPPED')
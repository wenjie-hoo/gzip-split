from asyncio.constants import LOG_THRESHOLD_FOR_CONNLOST_WRITES
from bz2 import decompress
import gzip
import os
from itertools import islice
from natsort import natsorted


def get_contents(intput_file):
    with gzip.open(intput_file, 'rb') as f:
        file_content = f.read()
        lines_of_content = file_content.split(b"\n")
        return lines_of_content


def write_gz(input_path, content):
    with gzip.open(input_path, 'r', 0) as f:
        file_content = f.read()
    with gzip.open(input_path, 'wb', 0) as f:
        f.write(file_content)
        f.write(content)


def gzip_wirte(path, fill):
    with gzip.open(path, 'r', 0) as f:
        content = f.read()
    with gzip.open(path, 'wb', 0) as f:
        f.write(content)
        f.write(fill)


def split_file(lines_of_content, pre_name, path):
    list = ''.encode('ascii')
    number = 0
    count = 0
    countlen = 0
    for i in range(len(lines_of_content)):
        i_len = len(lines_of_content[i])
        if (count + countlen + i_len) > specified_size - 15000:
            file_name = path+'/'+pre_name+'_'+str(number)+'.gz' #linux
            # file_name = path + '\\' + pre_name + '_' + str(number) + '.gz'  # windows
            list += lines_of_content[i] + b'\n'
            with gzip.open(file_name, 'r', 0) as f:
                content = f.read()
            with gzip.open(file_name, 'wb', 0) as f:
                f.write(content)
                f.write(list)
            gz_size = os.stat(file_name)
            list = ''.encode('ascii')
            number += 1
            count = 0
            countlen = 0
            continue
        if (count % 5000 == 0):
            file_name = path+'/'+pre_name+'_'+str(number)+'.gz' #linux
            # file_name = path + '\\' + pre_name + '_' + str(number) + '.gz'  # windows
            try:
                with gzip.open(file_name, 'rb', 0) as f:
                    content = f.read()
                with gzip.open(file_name, 'wb', 0) as f:
                    f.write(content)
                    f.write(list)
            except IOError as e:
                with gzip.open(file_name, 'wb', 0) as f:
                    f.write(list)
            list = ''.encode('ascii')
        list += lines_of_content[i] + b'\n'
        # list = b"%s%s"%(list,lines_of_content[i] + b'\n')
        count += 1
        countlen += i_len
        # print(count)
    file_name = path+'/'+pre_name+'_'+str(number)+'.gz' #linux
    # file_name = path + '\\' + pre_name + '_' + str(number) + '.gz'  # windows
    with gzip.open(file_name, 'rb', 0) as f:
        content = f.read()
    with gzip.open(file_name, 'wb', 0) as f:
        f.write(content)
        f.write(list)


def total_lines(file):
    with gzip.open(file, 'rb') as f:
        for i, l in enumerate(f):
            pass
        return i + 1


def fill_zero(number):
    zero_list = ''
    for i in range(number):
        zero_list = zero_list + '0'
    return zero_list


def gzip_fill(path, listfill):
    with gzip.open(path, 'r', 0) as f:
        content = f.read()
    with gzip.open(path, 'wb', 0) as f:
        f.write(content)
        f.write(listfill.encode('ascii'))


def fill_tail(splitted_path):
    gz_size = os.stat(splitted_path)
    fill_number = 134217728 - gz_size.st_size
    listfill = fill_zero(100000)
    count = fill_number
    for i in range(fill_number):
        if (count < 140000):
            break
        if (i % 100000 == 0):
            with gzip.open(splitted_path, 'r', 0) as f:
                content = f.read()
            with gzip.open(splitted_path, 'wb', 0) as f:
                f.write(content)
                f.write(listfill.encode('ascii'))
            gz_size = os.stat(splitted_path)
            count = 134217728 - gz_size.st_size
    gz_size = os.stat(splitted_path)
    fill_number = 134217728 - gz_size.st_size
    listfill = fill_zero(10000)
    count = fill_number
    for i in range(fill_number):
        if (count < 10000):
            break
        if (i % 10000 == 0):
            with gzip.open(splitted_path, 'r', 0) as f:
                content = f.read()
            with gzip.open(splitted_path, 'wb', 0) as f:
                f.write(content)
                f.write(listfill.encode('ascii'))
            gz_size = os.stat(splitted_path)
            count = 134217728 - gz_size.st_size
    gz_size = os.stat(splitted_path)
    fill_number = 134217728 - gz_size.st_size
    listfill = fill_zero(fill_number)
    with gzip.open(splitted_path, 'rb', 0) as f:
        content = f.read()
    with gzip.open(splitted_path, 'wb', 0) as f:
        f.write(content)
        f.write(listfill.encode('ascii'))
    # gz_size = os.stat(splitted_path)
    # gzip_fill(splitted_path)


def decompressGZ(file, folder):
    file_path = folder + file  # linux
    # file_path = folder + '\\'+file  #windows
    print('file_path:', file_path)
    with gzip.open(file_path, 'rb') as f:
        lines = f.readlines()
        for line in lines:
            if not line.startswith(b'000'):
                df.write(line)


if __name__ == "__main__":
    done = False
    while not done:
        choice_mode = input(
            'compress files please input "c"\
            \ndecomprss files please input "d"\
            \ninput "q" for quit\
             \n ')
        if choice_mode == 'c':
            folder = input('input file path:')
            # folder = './'
            # specified_size = int(input('intput enter expected size (bytes):'))
            specified_size = 134217728
            start_line = 0
            for file in os.listdir(folder):
                if file.endswith('.gz'):
                    filename = folder + file
                    pre_name, suefix_name = os.path.splitext(file)
                    # print('pre_name:', pre_name, ' suefix_name:', suefix_name)
                    path = folder + pre_name
                    if not os.path.exists(path):
                        os.mkdir(path)
                    lines_of_contents = get_contents(filename)
                    # total_rows = total_lines(filename)
                    split_file(lines_of_contents, pre_name, path)
                    total_files = len(os.listdir(path))
                    splitted_dir = os.listdir(path)
                    splitted_dir_list = natsorted(splitted_dir)
                    print('splitted_dir_list:',splitted_dir_list)
                    for splitted_file in splitted_dir_list:
                        splitted_path = path + '/' + splitted_file  # linux
                        # splitted_path = path + '\\'+splitted_file  # windows
                        if splitted_file.endswith(".gz"):
                            if total_files == 1:
                                pass
                            else:
                                total_files -= 1
                                fill_tail(splitted_path)
            done = True
        elif choice_mode == 'd':
            folder = input('input file path:')
            list_dir = os.listdir(folder)
            file_list = natsorted(list_dir)
            # list_dir.sort()(key = lambda x: int(x[:-4]))
            folder_filter = folder[:-1]
            de_file_name, suefix_name = os.path.splitext(folder_filter)
            # print('de_file_name:',de_file_name)
            decompress_file = de_file_name + '.json'
            with open(decompress_file, 'wb') as df:
                for file in file_list:
                    if file.endswith('gz'):
                        decompressGZ(file, folder)
            done = True
        elif choice_mode == 'q':
            break
        else:
            print('please try again')


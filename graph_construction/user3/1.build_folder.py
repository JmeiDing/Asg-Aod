import os
import glob

inputpath = './data/rawdata/UBA1_3ss/user_*/*.csv'
outputpath = './data/rawdata/UBA1_3ss/generation_data_batch/'


for file in glob.glob(inputpath):
    print(file)
    user = file.split('/')[7].split('.csv')[0]
    print(user)

    user_folder = outputpath + user
    print(user_folder)

    folder = os.path.exists(user_folder)

    if not folder:
        os.mkdir(user_folder)
    else:
        print('There is this folder')

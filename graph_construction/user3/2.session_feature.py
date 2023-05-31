import numpy as np
import pandas as pd
import os, glob


inputpath = './data/rawdata/UBA1_3ss/user_*/*.csv'
outputpath = './data/rawdata/UBA1_3ss/session_node/'


column = ['operation', 'Non-working days', 'Working_days non-working_hours', 'Working_days working_hours',
          'activity_logon', 'http_keyboard','http_keylogging', 'http_keyboard_num', 'http_keylogging_num',
          'activity_device', 'device_num','filename_exe','filename_exe_num','filename_zip','filename_doc',
          'filename_txt','filename_pdf','filename_jpg', 'file_keyboard', 'file_keylogging', 'file_keyboard_num',
          'file_keylogging_num','day1','day2', 'session_indicator', 'targets', 'graph_indicator']

for file in glob.glob(inputpath):
    df = pd.read_csv(file, encoding='utf-8')
    name = file.split('/')[7]

    # Reconstructing data sets
    redata = np.zeros((len(df), len(column)))
    redata = pd.DataFrame(redata, columns=column)
    redata['operation'] = df['operation']
    redata['Non-working days'] = df['Non-working days']
    redata['Working_days non-working_hours'] = df['Working_days non-working_hours']
    redata['Working_days working_hours'] = df['Working_days working_hours']
    redata['activity_logon'] = df['activity_logon']
    redata['http_keyboard'] = df['http_keyboard']
    redata['http_keylogging'] = df['http_keylogging']
    redata['http_keyboard_num'] = 0
    redata['http_keylogging_num'] = 0
    redata['activity_device'] = df['activity_device']
    redata['device_num'] = 0
    redata['filename_exe'] = df['filename_exe']
    redata['filename_exe_num'] = 0
    redata['filename_zip'] = df['filename_zip']
    redata['filename_doc'] = df['filename_doc']
    redata['filename_txt'] = df['filename_txt']
    redata['filename_pdf'] = df['filename_pdf']
    redata['filename_jpg'] = df['filename_jpg']
    redata['file_keyboard'] = df['file_keyboard']
    redata['file_keylogging'] = df['file_keylogging']
    redata['file_keyboard_num'] = 0
    redata['file_keylogging_num'] = 0
    redata['day1'] = df['day1']
    redata['day2'] = df['day2']
    redata['session_indicator'] = df['session_indicator']
    redata['targets'] = df['targets']
    redata['graph_indicator'] = df['graph_indicator']

    #Extract session type
    session_data = df['session_indicator'].tolist()
    session_type = []
    for i in range(len(session_data)):
        if session_data[i] not in session_type:
            session_type.append(session_data[i])

    #Counting the number of abnormal URLs and device in a session
    session_http_keyboard_num = []
    session_http_keylogging_num = []
    session_device_num = []
    session_filename_exe_num = []
    session_file_keyboard_num = []
    session_file_keylogging_num = []
    for i in session_type:
        row = redata.loc[(redata['session_indicator'] == i)]
        http_keyboard_num = 0
        http_keylogging_num = 0
        device_num = 0
        filename_exe_num = 0
        file_keyboard_num = 0
        file_keylogging_num = 0

        for indexs in row.index:
            row_data = row.loc[indexs].values.tolist()
            if row_data[0] == 'http':
                if row_data[5] == 1:
                    http_keyboard_num += 1
                if row_data[6] == 1:
                    http_keylogging_num += 1

            if row_data[0] == 'device':
                device_num += 1

            if row_data[0] == 'file':
                if row_data[11] == 1:
                    filename_exe_num += 1
                if row_data[18] == 1:
                    file_keyboard_num += 1
                if row_data[19] == 1:
                    file_keylogging_num += 1

        session_http_keyboard_num.append(http_keyboard_num)
        session_http_keylogging_num.append(http_keylogging_num)
        session_device_num.append(device_num)
        session_filename_exe_num.append(filename_exe_num)
        session_file_keyboard_num.append(file_keyboard_num)
        session_file_keylogging_num.append(file_keylogging_num)

    session_http_keyboard_num = pd.DataFrame(session_http_keyboard_num, columns=['http_keyboard_num'])
    session_http_keylogging_num = pd.DataFrame(session_http_keylogging_num, columns=['http_keylogging_num'])
    session_device_num = pd.DataFrame(session_device_num, columns=['device_num'])
    session_filename_exe_num = pd.DataFrame(session_filename_exe_num, columns=['filename_exe_num'])
    session_file_keyboard_num = pd.DataFrame(session_file_keyboard_num, columns=['file_keyboard_num'])
    session_file_keylogging_num = pd.DataFrame(session_file_keylogging_num, columns=['file_keylogging_num'])

    #Build empty fill_data[], which holds session nodes
    data = np.zeros((len(session_type), len(column)))
    data = pd.DataFrame(data, columns=column)
    fill_data = []
    num = 5
    for indexs in data.index:
        row_data = data.loc[indexs].values.tolist()
        for col in range(0, len(column)):
            row_data[col] = num
        fill_data.append(row_data)
        num += 1
    fill_data = pd.DataFrame(fill_data, columns=column)

    #Add node number
    count = 0
    session_indicator = []
    for indexs in fill_data.index:
        row_data = fill_data.loc[indexs].values.tolist()
        # print(row_data)
        row_data[24] = session_type[count]
        session_indicator.append(row_data)
        count += 1
    session_indicator = pd.DataFrame(session_indicator, columns=column)

    session_indicator['targets'] = df['targets']
    session_indicator['graph_indicator'] = df['graph_indicator']
    session_indicator['http_keyboard_num'] = session_http_keyboard_num
    session_indicator['http_keylogging_num'] = session_http_keylogging_num
    session_indicator['device_num'] = session_device_num
    session_indicator['filename_exe_num'] = session_filename_exe_num
    session_indicator['file_keyboard_num'] = session_file_keyboard_num
    session_indicator['file_keylogging_num'] = session_file_keylogging_num

    combined_csv = pd.concat([redata, session_indicator], axis=0, ignore_index=True)
    print(combined_csv)
    combined_csv.to_csv(outputpath + name, index=False, encoding='utf_8')
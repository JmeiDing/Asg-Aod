import numpy as np
import pandas as pd
import os, glob



inputpath = './data/rawdata/UBA1_3ss/user_*/*.csv'
outputpath = './data/rawdata/UBA1_3ss/session_node/'

column = ['operation', 'Non-working days', 'Working_days non-working_hours', 'Working_days working_hours','activity_logon',
          'url','http_num', 'activity_device','device_num', 'session_indicator','targets','graph_indicator']

for file in glob.glob(inputpath):
    df = pd.read_csv(file, encoding='utf-8')
    name = file.split('/')[7]
    print(name)

    # Reconstructing data sets
    redata = np.zeros((len(df), len(column)))
    redata = pd.DataFrame(redata, columns=column)
    redata['operation'] = df['operation']
    redata['Non-working days'] = df['Non-working days']
    redata['Working_days non-working_hours'] = df['Working_days non-working_hours']
    redata['Working_days working_hours'] = df['Working_days working_hours']
    redata['activity_logon'] = df['activity_logon']
    redata['url'] = df['url']
    redata['http_num'] = 0
    redata['activity_device'] = df['activity_device']
    redata['device_num'] = 0
    redata['session_indicator'] = df['graph_indicator']
    redata['targets'] = df['targets']
    redata['graph_indicator'] = df['graph_indicator']


    #Extract session type
    session_data = redata['session_indicator'].tolist()
    session_type = []
    for i in range(len(session_data)):
        if session_data[i] not in session_type:
            session_type.append(session_data[i])
    print(session_type)

    #Counting the number of abnormal URLs and device in a session
    session_http_num = []
    session_device_num = []
    for i in session_type:
        row = redata.loc[(redata['session_indicator'] == i)]
        http_num = 0
        device_num = 0
        for indexs in row.index:
            row_data = row.loc[indexs].values.tolist()
            if row_data[0] == 'http':
                if row_data[5] == 1:
                    http_num += 1

            if row_data[0] == 'device':
                device_num += 1

        session_http_num.append(http_num)
        session_device_num.append(device_num)
    session_http_num = pd.DataFrame(session_http_num, columns=['http_num'])
    session_device_num = pd.DataFrame(session_device_num, columns=['device_num'])
    print(session_http_num)
    print(session_device_num)

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
    print(fill_data)
    print('-------')

    #增加节点编号
    count = 0
    session_indicator = []
    for indexs in fill_data.index:
        row_data = fill_data.loc[indexs].values.tolist()
        # print(row_data)
        row_data[9] = session_type[count]
        session_indicator.append(row_data)
        count += 1
    session_indicator = pd.DataFrame(session_indicator, columns=column)

    session_indicator['targets'] = redata['targets']
    session_indicator['graph_indicator'] = redata['graph_indicator']
    session_indicator['http_num'] = session_http_num
    session_indicator['device_num'] = session_device_num

    print(session_indicator)

    combined_csv = pd.concat([redata, session_indicator], axis=0, ignore_index=True)
    print(combined_csv)
    combined_csv.to_csv(outputpath + name, index=False, encoding='utf_8')
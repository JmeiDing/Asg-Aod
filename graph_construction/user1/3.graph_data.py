import pandas as pd
import os, glob

inputpath = './data/rawdata/UBA1_3ss/session_node/*.csv'
outputpath = './data/rawdata/UBA1_3ss/generation_data_batch/'


def load_node_csv(file):
    df = pd.read_csv(file, encoding = "ISO-8859-1", on_bad_lines='skip')
    print(df.columns)

    #save file
    user = file.split('/')[7].split('.csv')[0]
    savepath = outputpath + user

    mapping = {index: i for i, index in enumerate(df.index)}

    #The indiccator of node
    graph_indicator = df['graph_indicator']
    assert len(df) == len(graph_indicator)
    graph_indicator.to_csv(savepath+'/graph_indicator.csv', index=False, encoding='utf_8')

    #The label of graph
    graph_labels = df['targets'].unique()
    graph_labels = pd.DataFrame(graph_labels, columns=['graph_labels'])
    graph_labels.to_csv(savepath+'/graph_labels.csv',index=False, encoding='utf_8')

    #The attributes of node
    node_attributes = df.iloc[:, 1:9]
    assert len(df) == len(node_attributes)
    node_attributes.to_csv(savepath+'/node_attributes.csv',index=False, encoding='utf_8')

    return mapping


def load_edge_csv(file):
    df = pd.read_csv(file, encoding='ISO-8859-1', on_bad_lines='skip')
    print('df',len(df))

    # savepath
    user = file.split('/')[7].split('.csv')[0]
    savepath = outputpath + user


    # the data of node
    data = []
    for indexs in df.index:
        row_data = df.loc[indexs].values.tolist()
        print(row_data)
        operation = row_data[0]
        if operation =='logon':
            data.append(0)
        elif operation == 'http':
            data.append(1)
        elif operation == 'email':
            data.append(2)
        elif operation == 'device':
            data.append(3)
        elif operation == 'file':
            data.append(4)
        else:
            # the node of session
            data.append(operation)
    node_label = pd.DataFrame(data,columns=['node_label'])
    assert len(df) == len(node_label)
    node_label.to_csv(savepath +'/node_label.csv',index=False, encoding='utf_8')

    # index-edge
        # the type of session
    session_data = df['session_indicator'].tolist()
    session_type = []
    for i in range(len(session_data)):
        if session_data[i] not in session_type:
            session_type.append(session_data[i])
    print(session_type)

    interval = len(df) - len(session_type)
    print(interval)

    # activity-edge
    index_edge1 = []
    for index in range(interval - 1):
        src = index
        dst = index + 1
        edge = [src,dst]
        index_edge1.append(edge)
    load_index_edge1 = pd.DataFrame(index_edge1, columns=['src', 'dst'])

    # aggregate-edge
    # activity node
    d1 = df.iloc[0:interval, :]
    # session node
    d2 = df.iloc[interval:len(df), :]
    index_edge2 = []
    for i in d1.index:
        row1 = d1.loc[i].values.tolist()
        activity = row1[9]

        for j in d2.index:
            row2 = d2.loc[j].values.tolist()
            session = row2[9]

            if session == activity:
                src = i
                dst = j
                edge = [src, dst]
                index_edge2.append(edge)
    load_index_edge2 = pd.DataFrame(index_edge2, columns=['src', 'dst'])

    # association-edge
    index_edge3 = []
    for k in range(len(session_type)-1):
        src = k + interval
        dst = k + 1 + interval
        edge = [src, dst]
        index_edge3.append(edge)
    load_index_edge3 = pd.DataFrame(index_edge3, columns=['src', 'dst'])

    # all-edge
    load_index_edge = [load_index_edge1,load_index_edge2,load_index_edge3]
    load_index_edge = pd.concat(load_index_edge, axis=0, ignore_index=True)
    load_index_edge.to_csv(savepath+'/graph_A.csv',index=False, encoding='utf_8')
    return load_index_edge

for file in glob.glob(inputpath):

    mapping = load_node_csv(file)
    load_index_edge = load_edge_csv(file)

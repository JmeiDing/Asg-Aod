import numpy as np
import pandas as pd
import os, glob
import os


inputpath = './data/rawdata/UBA1_3ss/generation_data_batch/'
savepath = "./data/graphdata"


total_graph_indicator = []
total_graph_A = []
total_node_labels = []
total_graph_labels = []
total_node_attributes = []
graph_indicator_label = {}

node_num = 0
node_num_list = [0]
count = 0

for fatherfile in os.listdir(inputpath):
    childpath = inputpath +'/'+ fatherfile
    #the file is consist of 5 graph data obtained by a user
    files = os.listdir(childpath)

    #read file path
    #list()[0] is the index of the files, here, graph_indicator is only one, so graph_indicator = graph_indicator.csv
    graph_indicator = list(filter(lambda f: f.find('graph_indicator') >= 0, files))[0]
    graph_indicator_path = childpath + "/" + graph_indicator

    graph_A = list(filter(lambda f: f.find('graph_A') >= 0, files))[0]
    graph_A_path = childpath + "/" + graph_A

    node_labels = list(filter(lambda f: f.find('node_label') >= 0, files))[0]
    node_labels_path = childpath + "/" + node_labels

    graph_labels = list(filter(lambda f: f.find('graph_labels') >= 0, files))[0]
    graph_labels_path = childpath + "/" + graph_labels

    node_attributes = list(filter(lambda f: f.find('node_attributes') >= 0, files))[0]
    node_attributes_path = childpath + "/" + node_attributes

    # graph_indicator
    graph_indicator_data = pd.read_csv(graph_indicator_path)
    graph_indicator_data = graph_indicator_data.iloc[:, 0]
    for i in range(len(graph_indicator_data)):
        total_graph_indicator.append(graph_indicator_data[i])

    node_num += len(graph_indicator_data)
    node_num_list.append(node_num)

    # graph_A
    graph_A_data = pd.read_csv(graph_A_path)
    graph_A_data = graph_A_data + node_num_list[count]
    graph_A_data = graph_A_data.values.tolist()
    for i in range(len(graph_A_data)):
        total_graph_A.append(graph_A_data[i])
    count = count + 1

    # node_labels
    node_labels_data = pd.read_csv(node_labels_path)
    node_labels_data = node_labels_data.iloc[:, 0]
    assert len(graph_indicator_data) == len(node_labels_data)
    for i in range(len(node_labels_data)):
        total_node_labels.append(node_labels_data[i])

    # graph_labels
    graph_labels_data = pd.read_csv(graph_labels_path)
    graph_labels_data = graph_labels_data.iloc[:, 0]
    for i in range(len(graph_labels_data)):
        total_graph_labels.append(graph_labels_data[i])


    # node_attributes
    node_attributes_data = pd.read_csv(node_attributes_path)
    node_attr_num = node_attributes_data.iloc[:, 0]
    assert len(graph_indicator_data) == len(node_attr_num)
    for indexs in node_attributes_data.index:
        node_attr = node_attributes_data.loc[indexs].values.tolist()
        total_node_attributes.append(node_attr)

    # graph_indicator_label
    graph_ids = graph_indicator_data[0]
    graph_labels = graph_labels_data[0]
    if graph_indicator not in graph_indicator_label:
        graph_indicator_label[graph_ids] = []
    graph_indicator_label[graph_ids] = graph_labels


total_graph_indicator = pd.DataFrame(total_graph_indicator)
total_graph_indicator.to_csv(savepath + '/graph_indicator.txt',index=False, encoding='utf_8',header=None)

total_graph_A = pd.DataFrame(total_graph_A)
total_graph_A.to_csv(savepath + '/graph_A.txt',index=False, encoding='utf_8',header=None)

total_node_labels = pd.DataFrame(total_node_labels)
total_node_labels.to_csv(savepath + '/node_labels.txt',index=False, encoding='utf_8',header=None)

total_graph_labels = pd.DataFrame(total_graph_labels)
total_graph_labels.to_csv(savepath + '/graph_labels.txt',index=False, encoding='utf_8',header=None)

total_node_attributes = pd.DataFrame(total_node_attributes)
total_node_attributes.to_csv(savepath + '/node_attributes.txt',index=False, encoding='utf_8',header=None)

filename = open(savepath + '/graph_indicator_label.txt', 'w')  #dict转txt
for k, v in graph_indicator_label.items():
    print(k, v)
    filename.write(str(k) + ':' + str(v))
    filename.write('\n')
filename.close()
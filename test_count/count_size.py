# -*- coding: utf-8 -*
import json
import os
import datetime
import copy
import collections

def get_requests(tracefiles):#按时间顺序处理请求
    ret = []
    with open(tracefiles,'r') as f:
        requests = json.load(f)
    for request in requests:
        method =request['http.request.method']
        uri = request['http.request.uri']
        if ('GET' == method or 'PUT' == method) and ('blobs' in uri) and (len(uri.split('/')) >= 5):
            if 'http.response.written' in request:
                size = request['http.response.written']
            else:
                size = 1
            if size > 0:
                timestamp = datetime.datetime.strptime(request['timestamp'], '%Y-%m-%dT%H:%M:%S.%fZ')
                duration = request['http.request.duration']
                client = request['http.request.remoteaddr']
                r = {
                    'timestamp':timestamp,
                    'uri':uri,
                    'size':size,
                    'method':method,
                    'delay':duration,
                    'client':client
                }
                ret.append(r)
    ret.sort(key=lambda x:x['timestamp'])
    if ret!= []:
        begin = ret[0]['timestamp']
        for r in ret:
            r['timestamp'] = (r['timestamp']-begin).total_seconds()
        return ret
    else:
        return ret

def exe_trace(file_name):
    requests = []
#    trace_files=["/trace/data_centers/dal09/prod-dal09-logstash-2017.06.20-0.json",
#                "/trace/data_centers/dal09/prod-dal09-logstash-2017.06.20-1.json",
#                "/trace/data_centers/dal09/prod-dal09-logstash-2017.06.20-2.json",
#                "/trace/data_centers/dal09/prod-dal09-logstash-2017.06.20-3.json"]
    trace_files=[]
    size_list = [0,0,0,0,0,0,0,0,0,0]
#    root='/home/zc/trace/'+file_name
    root='/home/zc/old_trace/data_centers/'+file_name
    paths = os.listdir(root)
    for path in paths:
        trace_files.append(root+'/'+path)
    i = 0
    l = len(trace_files)
    count = 0
    j = 0
    datas = []
    for file in trace_files:
        if 1.*i / l > 0.01:
            count += 1
            i = 0
            print(str(count) + '% done')
        i += 1
        requests = get_requests(file)
        for request in requests:
            if request['size']<10:
                size_list[0] += 1
            elif request['size']<100:
                size_list[1] += 1
            elif request['size']<1000:
                size_list[2] += 1
            elif request['size']<10000:
                size_list[3] += 1
            elif request['size']<100000:
                size_list[4] += 1
            elif request['size']<1000000:
                size_list[5] += 1
            elif request['size']<10000000:
                size_list[6] += 1
            elif request['size']<100000000:
                size_list[7] += 1
            elif request['size']<1000000000:
                size_list[8] += 1
            elif request['size']<10000000000:
                size_list[9] += 1
        j += 1
    with open('size_'+file_name+'.txt', 'w') as f:
        f.write(str(j)+'\n')
        f.write(str(size_list)+'\n')

def main():
    filenames=['dev-mon01','prestage-mon01','syd01','fra02','stage-dal09','lon02','dal09']
    for number in range(6,7):
        exe_trace(filenames[number])

if __name__ == '__main__':
    main()

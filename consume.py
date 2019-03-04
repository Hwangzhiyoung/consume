# -*- coding: UTF-8 -*-

import Queue
import threading
import time
import os
import ast
import json

parse_thread_num=100
total_data_num=0
exitFlag=0

def reportData2(data_row):
    # print("data_row:%s"%data_row)
    time.sleep(0.1)
    return

class Consumer(threading.Thread):
    """
    Consumer thread 消费线程
    """
    def __init__(self, t_name,thread_id, queue):
        threading.Thread.__init__(self, name=t_name)
        self.data = queue
        self.thread_id = int(thread_id)

    def run(self):
        global finish_flag
        compensate_success_num = 0
        compensate_fail_num = 0
        while not exitFlag:
            if not self.data.empty():
                data_row = self.data.get()
                if self.data.qsize()%1000 == 0:
                    print ('percent: [%d/%d]'%((total_data_num - self.data.qsize()),total_data_num))
                reportData2(data_row)
            

def parseDataJson(filepath):
    results = []
    file = open(filepath)
    for line in file:
        tempdict = ast.literal_eval(line)
        results.append(tempdict)
    return results

def main(datadir):
    if os.path.isfile(datadir)==0:
        print("The dir %s is not exist!"%datadir)
        return
    queue = Queue.Queue()
    global queueLock
    queueLock = threading.Lock()
    
    phonedicts=parseDataJson(datadir)
    j = json.dumps(phonedicts)
    phonedicts = json.loads(j)
    #入队列
    for phonedict in phonedicts:
        queue.put(phonedict,True)
    global total_data_num
    total_data_num = queue.qsize()
    if total_data_num<0:
        return

    consumers = []
    #多线程消费
    for i in range(parse_thread_num):
        consumer = Consumer("Con.%d"%i, i,queue)
        consumer.start()
        consumers.append(consumer)
    
    # 等待队列清空
    while not queue.empty():
        pass

    # 通知线程是时候退出
    global exitFlag
    exitFlag = 1
    
    for consumer in consumers:
        consumer.join()

if __name__ == '__main__':
    datadir="./" + "touch_data_500000_20190110"
    start = time.time()
    main(datadir)
    end = time.time()
    print("Time used:",end - start)

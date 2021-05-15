import atexit
import json
import os
import re
import signal
import subprocess
import sys
import time
from datetime import datetime

import psutil

__author__ = '大鱼哥'
# Sponsor/Support(赞助/支持) (XCH):  xch13ejpkjskwd46dz4r3979gjghpdhs29m59r5sgtscfm6hdds467cqpc2749
# Youtube频道 : https://www.youtube.com/channel/UCoot7-1rYV18wVNbQrR8JXA

# 'chia_path' :           奇亚程序目录 windows下在 'C:/Users/XXXXXXX/AppData/Local/chia-blockchain/app-1.1.X/resources/app.asar.unpacked/daemon/', linux下可能在 '/usr/lib/chia-blockchain/resources/app.asar.unpacked/daemon/'
# farmer_public_key       你的farmer_public_key,无需导入钱包,无需住记词
# pool_public_key         你的pool_public_key,无需导入钱包,无需住记词
# global_start_interval   全局(所有队列)启动间隔,单位秒,每个启动线程都需要满足此条件
# global_phase1_cnt       全局(所有队列)处于phase1阶段的线程总量不能超过这个值
# pools.pool_name         队列名称
# pools.pool_run_cnt      队列中允许同时运行的总数
# pools.pool_start_interval  队列启动间隔,单位秒
# pools.threads           P盘任务的线程数
# pools.thread_k          P盘任务的K值
# pools.thread_mem        P盘任务的内存占用值
# pools.thread_tmp_path   P盘任务的tmp目录
# pools.thread_tmp_path2  P盘任务的tmp2目录
# pools.thread_target     P盘任务的目标目录
plot_config = {}
plot_stat = {}

def signal_handler(signal,frame):
    print('You pressed Ctrl+C!')
    print('use kill -9 to exit!')
 

def kill(proc_pid):
    process = psutil.Process(proc_pid)
    for proc in process.children(recursive=True):
        proc.kill()
    process.kill()

def get_phase_from_logfile(logfile):
    # Map from phase number to subphase number reached in that phase.
    # Phase 1 subphases are <started>, table1, table2, ...
    # Phase 2 subphases are <started>, table7, table6, ...
    # Phase 3 subphases are <started>, tables1&2, tables2&3, ...
    # Phase 4 subphases are <started>
    phase_subphases = {}

    with open(logfile, 'r') as f:
        for line in f:
            # "Starting phase 1/4: Forward Propagation into tmp files... Sat Oct 31 11:27:04 2020"
            m = re.match(r'^Starting phase (\d).*', line)
            if m:
                phase = int(m.group(1))
                phase_subphases[phase] = 0

            # Phase 1: "Computing table 2"
            m = re.match(r'^Computing table (\d).*', line)
            if m:
                phase_subphases[1] = max(phase_subphases[1], int(m.group(1)))

            # Phase 2: "Backpropagating on table 2"
            m = re.match(r'^Backpropagating on table (\d).*', line)
            if m:
                phase_subphases[2] = max(phase_subphases[2], 7 - int(m.group(1)))

            # Phase 3: "Compressing tables 4 and 5"
            m = re.match(r'^Compressing tables (\d) and (\d).*', line)
            if m:
                phase_subphases[3] = max(phase_subphases[3], int(m.group(1)))

            # TODO also collect timing info:

            # "Time for phase 1 = 22796.7 seconds. CPU (98%) Tue Sep 29 17:57:19 2020"
            # for phase in ['1', '2', '3', '4']:
                # m = re.match(r'^Time for phase ' + phase + ' = (\d+.\d+) seconds..*', line)
                    # data.setdefault....

            # Total time = 49487.1 seconds. CPU (97.26%) Wed Sep 30 01:22:10 2020
            # m = re.match(r'^Total time = (\d+.\d+) seconds.*', line)
            # if m:
                # data.setdefault(key, {}).setdefault('total time', []).append(float(m.group(1)))

    if phase_subphases:
        phase = max(phase_subphases.keys())
        phase = (phase, phase_subphases[phase])
    else:
        phase = (0, 0)
    return phase

def init_pool():
    global plot_config
    global plot_stat
    print(sys.argv)
    if len(sys.argv) < 2 :
        print('run with config file!')
        exit()

    with open(sys.argv[1]) as cfg:
        plot_config = json.load(cfg)

    signal.signal(signal.SIGINT,signal_handler)
    atexit.register(cleanup_pool_thread)

    for pool in plot_config['pools'] :
        if not 'pool_cur_run_cnt' in pool :
            pool['pool_cur_run_cnt'] = 0
        if not 'pool_last_run_time' in pool :
            pool['pool_last_run_time'] = 0
        if not 'pool_run_data' in pool :
            pool_run_data = []
            for i in range(pool['pool_run_cnt']):
                print('i=', i)
                pool_run_data.append({'thread_no' : i+1, 'run_status' : 0})
            pool['pool_run_data'] = pool_run_data
    print(plot_config)

    plot_stat['start_time']

#全部队列的最近一次运行时间
def get_pools_last_run_time() :  
    t = 0
    for pool in plot_config['pools'] :
        if pool['pool_last_run_time'] > t :
            t = pool['pool_last_run_time']
    return t

#返回处于第一阶段的程序数量
def get_pools_global_phase1_cnt():  
    t = 0
    for pool in plot_config['pools'] :
        pool_run_datas = pool['pool_run_data']
        for pool_run_data in pool_run_datas :
            if pool_run_data['run_status'] == 1 :
                time_no_str = datetime.fromtimestamp(pool_run_data['start_time']).strftime("%Y%m%d%H%M%S")
                thread_id = pool['pool_name'] + '_' + "{:02d}".format(pool_run_data['thread_no']) + '_' + time_no_str
                logfile = "logs/" + thread_id + '.log'
                phase1 = get_phase_from_logfile(logfile)[0]
                #第一阶段
                if phase1 == 1:
                    t += 1
    return t

#检查全局条件
def global_req_check():
    pools_last_run_time = get_pools_last_run_time()
    t = int(datetime.now().timestamp())
    if t - pools_last_run_time < plot_config['global_start_interval'] :
        # print('global_start_interval check fail.')
        return False
    
    phase1_cnt = get_pools_global_phase1_cnt()
    print('phase1_cnt = ', phase1_cnt)
    
    if phase1_cnt >= plot_config['global_phase1_cnt'] :
        print('global_phase1_cnt check fail.')
        return False

    return True

def pool_req_check(pool):
    pool_run_cnt = pool['pool_run_cnt']
    pool_cur_run_cnt = pool['pool_cur_run_cnt']
    if pool_cur_run_cnt >= pool_run_cnt :
        # print('pool_cur_run_cnt check fail.')
        return False

    pool_start_interval = pool['pool_start_interval']
    t = int(datetime.now().timestamp())
    if t - pool['pool_last_run_time']  < pool_start_interval :
        # print('pool_start_interval check fail.')
        return False

    return True

def clean_tmp_files(path):
    print("clean tmp files in ", path)
    for root, dirs, files in os.walk(path):
        # print(files)
        for name in files:
            # print(name)
            #已完成的绘图文件 保留
            if name.endswith('.plot.2.tmp'):
                fsize = os.path.getsize(os.path.join(root, name))
                #大于100G可能是完成的绘图文件
                if fsize > 1024 * 1024 * 1024 * 100 :
                    print('found ' + os.path.join(root, name) + ', maybe finished plot file!')
                    continue
            if name.endswith('.tmp'):
                # print(os.path.join(root, name))
                try :
                    os.remove(os.path.join(root, name))
                except:
                    print('failed to remove ', os.path.join(root, name))
                    pass

def cleanup_pool_thread():
    print('cleanup_pool_thread... ')
    for pool in plot_config['pools'] :
        pool_run_datas = pool['pool_run_data']
        for pool_run_data in pool_run_datas :
            if pool_run_data['run_status'] == 1 :
                print('kill()')
                kill(pool_run_data['subprocess'].pid)

def start_pool_thread(pool) :
    cur_pool_run_data = {}
    pool_run_datas = pool['pool_run_data']
    for pool_run_data in pool_run_datas :
        if pool_run_data['run_status'] == 0 :
            cur_pool_run_data = pool_run_data
            break
    
    start_time = datetime.now()
    time_no_str = start_time.strftime("%Y%m%d%H%M%S")
    pool['pool_cur_run_cnt'] += 1
    pool['pool_last_run_time'] = int(start_time.timestamp())

    cur_pool_run_data['start_time'] = int(start_time.timestamp())
    cur_pool_run_data['run_status'] = 1

    thread_id = pool['pool_name'] + '_' + "{:02d}".format(cur_pool_run_data['thread_no']) + '_' + time_no_str

    thread_tmp_path = pool['thread_tmp_path'] + '/' + "{:02d}".format(cur_pool_run_data['thread_no']) + '/'
    thread_tmp_path2 = pool['thread_tmp_path2'] + '/' + "{:02d}".format(cur_pool_run_data['thread_no']) + '/'
    clean_tmp_files(thread_tmp_path)
    if thread_tmp_path != thread_tmp_path2 :
        clean_tmp_files(thread_tmp_path2)

    with open("logs/" + thread_id + '.log',"wb") as out:
        print('start_pool_thread... ')
        cmd = plot_config['chia_path'] + 'chia plots create -n 1 ' 
        cmd += '-f ' + plot_config['farmer_public_key'] + ' ' 
        cmd += '-p ' + plot_config['pool_public_key'] + ' ' 
        cmd += '-r ' + str(pool['threads']) + ' ' 
        cmd += '-k ' + str(pool['thread_k']) + ' ' 
        cmd += '-b ' + str(pool['thread_mem']) + ' ' 
        cmd += '-t ' + thread_tmp_path + '/ ' 
        cmd += '-2 ' + thread_tmp_path2 + '/ ' 
        cmd += '-d ' + pool['thread_target'] + ' ' 
        print(cmd)
        p1 = subprocess.Popen(cmd, shell=True, stdout=out, stderr=out, encoding="utf-8")
        cur_pool_run_data['subprocess'] = p1

def finish_pool_thread():
    # print('finish_pool_thread... ')
    for pool in plot_config['pools'] :
        pool_run_datas = pool['pool_run_data']
        for pool_run_data in pool_run_datas :
            if pool_run_data['run_status'] == 1 :
                #已运行结束
                if pool_run_data['subprocess'].poll() is not None :
                    pool_run_data['run_status'] = 0
                    pool['pool_cur_run_cnt'] -= 1
                    del pool_run_data['start_time']
                    del pool_run_data['subprocess']

def readline_count(file_name):
    return len(open(file_name).readlines())

def show_pool_status():
    print('----------------------------all pool status----------------------------------------------------')
    for pool in plot_config['pools'] :
        print(pool['pool_name'], ':')
        pool_run_datas = pool['pool_run_data']
        for pool_run_data in pool_run_datas :
            if pool_run_data['run_status'] == 0 :
                print('\t' + "{:02d}".format(pool_run_data['thread_no']) + ': waiting ...')
            else :
                time_no_str = datetime.fromtimestamp(pool_run_data['start_time']).strftime("%Y%m%d%H%M%S")
                thread_id = pool['pool_name'] + '_' + "{:02d}".format(pool_run_data['thread_no']) + '_' + time_no_str
                logfile = "logs/" + thread_id + '.log'
                print('\t' + "{:02d}".format(pool_run_data['thread_no']) + ': start_time=' + 
                    datetime.fromtimestamp(pool_run_data['start_time']).strftime("%Y-%m-%d %H:%M:%S") + ', progress=' + 
                    str(get_phase_from_logfile(logfile)) + '('+ "{:d}".format(int(readline_count(logfile) * 100 / 2626)) +'%)')
    print()
    print()

def pools_run():
    init_pool()

    while True:
        for pool in plot_config['pools'] :
            if global_req_check() and pool_req_check(pool) :
                start_pool_thread(pool)
                break

        finish_pool_thread()
        show_pool_status()
        time.sleep(5)

if __name__ == "__main__":
    pools_run()

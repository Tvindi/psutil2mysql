import PSUTIL_dbi_0_1 as dbi
import time
import psutil
import pprint
import collections

def pad(w):
    if len(w) == 1:
        w = '00000' + w
    elif len(w) == 2:
        w = '0000' + w
    elif len(w) == 3:
        w = '000' + w
    elif len(w) == 4:
        w = '00' + w
    return w


def poll(interval):
    # sleep some time
    time.sleep(interval)
    epoch_time = int(time.time())
    procs = []
    procs_status = {}
    for p in psutil.process_iter():
        try:
            p.dict = p.as_dict(['username', 'nice', 'memory_info',
                                'memory_percent', 'cpu_percent',
                                'cpu_times', 'name', 'status'])
            try:
                procs_status[str(p.dict['status'])] += 1
            except KeyError:
                procs_status[str(p.dict['status'])] = 1
        except psutil.NoSuchProcess:
            pass
        else:
            procs.append(p)

    """ print procs
    print k.dict
    """
    # return processes sorted by CPU percent usage
    processes = sorted(procs, key=lambda p: p.dict['cpu_percent'], reverse=True)
    return (processes, procs_status,epoch_time)


# =============== MAIN ===========================
if __name__ == "__main__":
    dbi.initmysql()
    dbi.setup()
    print 'done'

    pp = pprint.PrettyPrinter(indent=4)
    (prcs, ps, epoch) = poll(1)

# print prcs
# dir retuned by prcs instance
# ['__class__', '__delattr__', '__dict__', '__doc__', '__eq__', '__format__',
# '__getattribute__', '__hash__', '__init__', '__module__', '__ne__', '__new__',
# '__reduce__', '__reduce_ex__', '__repr__', '__setattr__', '__sizeof__',
# '__str__', '__subclasshook__', '__weakref__', '_create_time', '_exe', '_gone',
# '_hash', '_ident', '_init', '_last_proc_cpu_times', '_last_sys_cpu_times',
# '_name', '_oneshot_inctx', '_pid', '_ppid', '_proc', '_send_signal',
# 'as_dict', 'children', 'cmdline', 'connections', 'cpu_affinity',
# 'cpu_percent', 'cpu_times', 'create_time', 'cwd', 'dict', 'environ', 'exe',
# 'gids', 'io_counters', 'ionice', 'is_running', 'kill', 'memory_full_info',
# 'memory_info', 'memory_info_ex', 'memory_maps', 'memory_percent', 'name',
# 'nice', 'num_ctx_switches', 'num_fds', 'num_threads', 'oneshot' 'open_files',
# 'parent', 'pid', 'ppid', 'resume', 'rlimit', 'send_signal', 'status',
# 'suspend', 'terminal', 'terminate', 'threads', 'uids', 'username', 'wait']

    for k in prcs:
        #print ps
        #pp.pprint(k.gids())
        print "epoch " + str(epoch)

        ps_top = {}
        ps_top['epoch'] = epoch
        ps_top['username'] = k.username()
        ps_top['nice'] = k.nice()
        ps_top['memory_percent'] = k.memory_percent()
        ps_top['cpu_percent'] = k.cpu_percent()
        ps_top['name'] = k.name()
        ps_top['status'] = k.status()
        #print ps_top
        last_id = dbi.add_metrics(ps_top)
        k.memory_info()

        ps_cpu = {}

        pct = collections.namedtuple('pcputimes','user system children_user children_system')
        tmp = k.cpu_times()
        pct = tmp

        ps_cpu['top_id'] = last_id
        ps_cpu['user'] = pct.user
        ps_cpu['system'] = pct.system
        ps_cpu['children_user'] = pct.children_user
        ps_cpu['children_system'] = pct.children_system
        dbi.add_cpu_times(ps_cpu)

        ps_mem = {}

        pm = collections.namedtuple('pmem', 'rss vms shared text lib data dirty')
        tmp = k.memory_info()
        pm = tmp

        ps_mem['top_id'] = last_id
        ps_mem['rss'] = pm.rss
        ps_mem['vms'] = pm.vms
        ps_mem['shared'] = pm.shared
        ps_mem['text'] = pm.text
        ps_mem['lib'] = pm.lib
        ps_mem['data'] = pm.data
        ps_mem['dirty'] = pm.dirty
        dbi.add_memory_info(ps_mem)


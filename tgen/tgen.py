__author__ = 'li'

import random


# Traffic trace generator based on Facebook 2010 MapReduce 1 hour trace.
# input format
# L(1): <Number of ports in the fabric> <Number of coflows below (one per line)>
# L(i): 0<Coflow ID> 1<Arrival time (ms)> 2<Number of mappers> 3<Location of map-m>
#     4<Number of reducers> 5:end<Location of reduce-r:Shuffle megabytes of reduce-r>

# output format
# csv file
# flow_id flow_size flow_starttime coflow_id  src dst

# number of servers in the cluster

def tgen(trace='fb2010.txt', output='newtrace.txt', nhost=32, target_load=0.8):
    # trace load is ~78Gbps
    # network capacity is nhost*1Gbps assuming full-bisection bandwidth
    bisec = 1 * nhost
    size_scaling = target_load * bisec / 78
    # print size_scaling
    flow_id = 1
    totalsize = 0
    simtimes = []
    with open(trace, "rb") as fbtrace:
        with open(output, "wb") as f:
            f.write("flow_id,flow_size,starttime,coflow_id,src,dst\n")
            for l in fbtrace.readlines():
                l = l.rstrip('\n').split(' ')
                if len(l) == 2:
                    # ignore the first line
                    continue
                print l
                reducer_num_index = -1
                reducer_list_start = 10000000000
                src = []
                dst = []
                fsize = []
                coflow_id = -1
                start_time = -1
                for i, item in enumerate(l):
                    # print "Item {} is {}".format(i, item)
                    if i == 0:
                        coflow_id = int(item)
                    elif i == 1:
                        start_time = int(item)
                        simtimes.append(int(item))
                    elif i == 2:
                        num_src = int(item)
                        reducer_num_index = 2 + int(item) + 1
                        reducer_list_start = 2 + int(item) + 2
                    elif (i > 2) and (i < reducer_num_index):
                        src.append(item)
                    elif i == reducer_num_index:
                        num_dst = int(item)
                    elif i >= reducer_list_start:
                        d, s = item.split(':')
                        dst.append(int(d))
                        fsize.append(float(s))
                        totalsize += float(s)
                print "sources"
                print src
                print "destinations"
                print dst
                print "sizes"
                print fsize
                # generate flows
                if not (len(src) == int(num_src)):
                    print "Error len(src)={}, num_src={}".format(len(src), num_src)
                # avg_flow_size = fsize / len(src)
                for s in src:
                    for d, fs in zip(dst, fsize):
                        # flow_id flow_size flow_starttime coflow_id  src dst
                        avg_flow_size = size_scaling * float(fs) / len(src)
                        # print "flowsize is {}".format(avg_flow_size)
                        f.write('{0},{1},{2},{3},{4},{5}\n'.format(flow_id, avg_flow_size, start_time,
                                                                   coflow_id, int(s) % nhost,
                                                                   int(d) % nhost))
                        flow_id += 1
    simtime = max(simtimes) - min(simtimes)
    load = float(totalsize) / simtime
    print "Trace load is {} MB/ms = {} Gbps. Simulation time is {} s. Flow count is {}.".format(load, load * 8,
                                                                                                simtime / 1000, flow_id)


def filler(input='newtrace.txt', input_load=0.2, simtime=3600, output='filled.txt', target_load=0.8, nhost=32):
    additional_load = (target_load - input_load) * nhost  # Gbps
    additional_size = additional_load * simtime
    # flow_number = random.randint(1000, 10000)
    flow_sizes = list(decomp(int(additional_size)))
    with open(input, "rb") as infile:
        with open(output, "wb") as outfile:
            flow_id = 1
            for l in infile.readlines():
                outfile.write(l)
                flow_id += 1
            for fs in flow_sizes:
                outfile.write('{0},{1},{2},{3},{4},{5}\n'.format(flow_id, fs, random.randint(0, 3600000),
                                                                 -1, random.randint(1, nhost),
                                                                 random.randint(1, nhost)))
                flow_id += 1
    print "{} flows added, total flow count is {}.".format(len(flow_sizes), flow_id)


def decomp(i, maxsize=1000):
    while i > 0:
        n = min([random.randint(1, maxsize), random.randint(1, i)])
        yield n
        i -= n


if __name__ == "__main__":
    tgen(output='load20host32MR.txt', target_load=0.2)
    tgen(output='load40host32MR.txt', target_load=0.4)
    tgen(output='load60host32MR.txt', target_load=0.6)
    tgen(output='load80host32MR.txt', target_load=0.8)
    filler(input='load20host32MR.txt', input_load=0.2, output='mr20load40.txt', target_load=0.4)
    filler(input='load20host32MR.txt', input_load=0.2, output='mr20load60.txt', target_load=0.6)
    filler(input='load20host32MR.txt', input_load=0.2, output='mr20load80.txt', target_load=0.8)
    filler(input='load40host32MR.txt', input_load=0.4, output='mr40load60.txt', target_load=0.6)
    filler(input='load40host32MR.txt', input_load=0.4, output='mr40load80.txt', target_load=0.8)
    filler(input='load60host32MR.txt', input_load=0.6, output='mr60load80.txt', target_load=0.8)



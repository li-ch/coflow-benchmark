__author__ = 'li'

# Traffic trace generator based on Facebook 2010 MapReduce 1 hour trace.
# input format
# L(1): <Number of ports in the fabric> <Number of coflows below (one per line)>
# L(i): 0<Coflow ID> 1<Arrival time (ms)> 2<Number of mappers> 3<Location of map-m>
#     4<Number of reducers> 5:end<Location of reduce-r:Shuffle megabytes of reduce-r>

# output format
# csv file
# flow_id flow_size flow_starttime coflow_id  src dst

# number of servers in the cluster
NHOST = 4 * 8
flow_id = 1

with open('fb2010.txt', "rb") as fbtrace:
    with open('newtrace.txt', "wb") as f:
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
            for i, item in enumerate(l):
                print "Item {}".format(i)
                print item
                if i == 0:
                    coflow_id = item
                elif i == 1:
                    start_time = item
                    continue
                elif i == 2:
                    num_src = item
                    reducer_num_index = 2 + int(item) + 1
                    reducer_list_start = 2 + int(item) + 2
                elif i < reducer_num_index:
                    src.append(item)
                elif i == reducer_num_index:
                    num_dst = item
                elif i >= reducer_list_start:
                    d, s = item.split(':')
                    dst.append(d)
                    fsize.append(s)
            print "sources"
            print src
            print "destinations"
            print dst
            print "sizes"
            print fsize
            # generate flows


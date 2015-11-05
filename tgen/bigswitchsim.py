__author__ = 'li'
from numpy import zeros
# simulating a big switch like in Varys
# confirming validity of trace

# number of hosts
nhost = 320
# NIC capacity at the switch
# 1 Gbps = 1024 Mbps = 128 MBps = 128 MB / 1000 ms = 0.128 MB/ms
niccap = 0.128
# full bisection bandwidth
bisec = 320 * niccap

flows = []

inputfile = 'load40host120MR'
with open('{}.txt'.format(inputfile), 'rb') as infile:
    nline = 0
    for l in infile.readlines():
        # skip header line
        if nline == 0:
            nline += 1
            continue
        nline += 1

        line = l.rstrip('\n').split(',')
        # print "{},{},{},{},{},{}".format(line[0], line[1], line[2], line[3], line[4], line[5])
        f = {'flow_id': int(line[0]),
             'flow_size': float(line[1]),
             'start_time': int(line[2]),
             'coflow_id': int(line[3]),
             'src': int(line[4]),
             'dst': int(line[5])
             }
        flows.append(f)

nflows = sorted(flows, key=lambda k: k['start_time'])
del flows # keeping it clean
flows = nflows

print "First 10 flows:"
for f in flows[:10]:
    print f

print "total flows: {}".format(len(flows))

TMAX = 3650000
waiting = flows
running = []
finished = []
src_count = zeros(nhost + 1)
dst_count = zeros(nhost + 1)

step = 10000  # ms
for t in range(0, TMAX, step):
    if len(running) == 0:
        if t < waiting[0]['start_time']:
            print "Skipped {}".format(t)
            continue

    for f in waiting:
        if f['start_time'] <= t:
            # print "f start at {}, current time is {}".format(f['start_time'], t)
            f['sent'] = 0
            running.append(f)
            waiting.remove(f)
            src_count[f['src']] += 1
            dst_count[f['dst']] += 1

    for f in running:
        # fair share policy, no work conservation
        send_size = niccap * step / max([src_count[f['src']], dst_count[f['dst']]])
        f['sent'] += send_size
        # print "flow {} has sent {}, its size is {}".format(f['flow_id'], f['sent'], f['flow_size'])

        if f['sent'] > f['flow_size']:
            running.remove(f)
            finished.append(f)
            f['finish_time'] = t
            f['fct'] = t - f['start_time'] + step
            src_count[f['src']] -= 1
            dst_count[f['dst']] -= 1
            if dst_count[f['dst']] < 0:
                dst_count = 0
            if src_count[f['src']] < 0:
                src_count = 0

    if len(running) == 0 and len(waiting) == 0:
        break

    print "Simulation {:.2f}% Complete".format(100.0 * t / TMAX)
    print "{} waiting, {} running, {} finished".format(len(waiting), len(running), len(finished))

outputfile = 'out_{}.txt'.format(inputfile)
with open(outputfile, "wb") as outfile:
    outfile.write("flow_id,flow_size,start_time,coflow_id,src,dst,finish_time,fct\n")
    for f in finished:
        outfile.write('{0},{1},{2},{3},{4},{5},{6},{7}\n'.format(f['flow_id'],
                                                                 f['flow_size'],
                                                                 f['start_time'],
                                                                 f['coflow_id'],
                                                                 f['src'],
                                                                 f['dst'],
                                                                 f['finish_time'],
                                                                 f['fct']))

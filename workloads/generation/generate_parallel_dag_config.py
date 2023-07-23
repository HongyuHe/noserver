basic_parallel_header = '''
{
    "Comment": "parallel",
    "StartAt": [
        F_START
    ],
    "Functions": {
'''
single_parallel = '''
        F_START: {
            "Next": [
                F_NEXT
            ],
            "DurationMilli": [
                DURATION
            ],
            "MemoryMib": [MEM]
        },
'''


def generate_single_parallel(args, parallel_header):
    parallel_header = parallel_header.replace("F_START", args['start'])
    parallel_header = parallel_header.replace("F_NEXT", args['next'])
    parallel_header = parallel_header.replace("MEM", str(args['mem']))
    parallel_header = parallel_header.replace("DURATION", str(args['duration']))
    return parallel_header
    
def generate_parallel_header(args, parallel_header):
    parallel_header = parallel_header.replace("F_START", args['start'])
    return parallel_header
    
def generate_parallel_close(args):
    parallel_close = '''
        F_START: {
            "Next": [F_END],
            "DurationMilli": [
                DURATION
            ],
            "MemoryMib": [
                MEM
            ]
        },
'''
    parallel_close = parallel_close.replace("F_START", args['start'])
    parallel_close = parallel_close.replace("F_END", args['end'])
    # import pdb; pdb.set_trace()
    parallel_close = parallel_close.replace("DURATION", str(args['duration']))
    parallel_close = parallel_close.replace("MEM", str(args['mem']))
    return parallel_close
    
def generate_parallel_end(end):
    parallel_end = '''
        F_END: {
            "Next": [],
            "DurationMilli": [
                1000
            ],
            "MemoryMib": [
                170
            ]
        }
    }
}
'''
    return parallel_end.replace("F_END", end)
    
def generate_chain(len, mem, time, parallel_header, parallel_single):
    output = ''
    # TODO: add stochasticity to the mem and time of functions in the chain
    for i in range(eval(len)+1):
        if i == 0:
            output += generate_parallel_header({'start': '"F0"'}, parallel_header)
            arg_dict = dict()
            arg_dict['start'] = '"F{}"'.format(i)
            arg_dict['mem'] = mem
            arg_dict['duration'] = time
            arg_dict['next'] = ''
        elif i == eval(len):
            print("last one", len)
            arg_dict['next'] += '\t\t\t\t"F{}"'.format(i)
        elif i == 1:
            arg_dict['next'] += '"F{}",\n'.format(i)
        else: 
            arg_dict['next'] += '\t\t\t\t"F{}",\n'.format(i)
        

    output += generate_single_parallel(arg_dict, parallel_single)
        
    # append ending 
    for i in range(1, eval(len)+1):
        arg_dict['start'] = '"F{}"'.format(i) 
        arg_dict['end'] = '"F{}"'.format(eval(len)+1)
        output += generate_parallel_close(arg_dict)
        
    # output = output[:-2] #delete ','
    output += generate_parallel_end('"F{}"'.format(eval(len)+1))
    return output
        
def main():
    import argparse
    global basic_parallel_header
    global single_parallel
    parser = argparse.ArgumentParser(description='')
    parser.add_argument('-s', '--nstages', dest='nstages', action='store',
                      help='Number of stages of chain', default='4')
    parser.add_argument('-m', '--mem', dest='mem', action='store',
                      help='Memory for a single function (currently homogeneous)', default=170)
    parser.add_argument('-t', '--time', dest='time', action='store',
                      help='Time spent on a single function in ms (currently homogeneous)', default=1000)
    parser.add_argument('-o', '--output', dest='output', action='store',
                      help='File to store output', default='workloads/dags/test_parallel')
    opts = parser.parse_args()
    
    output = (generate_chain(opts.nstages, opts.mem, opts.time, basic_parallel_header, single_parallel))
    opts.output += '_s{}_m{}_t{}.json'.format(opts.nstages, opts.mem, opts.time)
    f = open(opts.output, "w")
    f.write(output)
    f.close()
    
if __name__ == "__main__":
    main()
    




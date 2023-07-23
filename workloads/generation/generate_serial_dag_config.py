basic_serial_header = '''
{
    "Comment": "serial",
    "StartAt": [
        F_START
    ],
    "Functions": {
'''
single_serial = '''
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

def generate_single_serial(args, serial_header):
    serial_header = serial_header.replace("F_START", args['start'])
    serial_header = serial_header.replace("F_NEXT", args['next'])
    serial_header = serial_header.replace("MEM", str(args['mem']))
    serial_header = serial_header.replace("DURATION", str(args['duration']))
    return serial_header
    
def generate_serial_header(args, serial_header):
    serial_header = serial_header.replace("F_START", args['start'])
    return serial_header
    
def generate_serial_end():
    serial_end = '''
    }
}
'''
    return serial_end
    

def generate_chain(len, mem, time, serial_header, serial_single):
    output = ''
    # TODO: add stochasticity to the mem and time of functions in the chain
    for i in range(eval(len)+1):
        if i == 0:
            output += generate_serial_header({'start': '"F0"'}, serial_header)
        arg_dict = dict()
        arg_dict['start'] = '"F{}"'.format(i)
        arg_dict['next'] = '"F{}"'.format(i+1)
        arg_dict['mem'] = mem
        arg_dict['duration'] = time
        output += generate_single_serial(arg_dict, serial_single)
        
    last_arg_dict = {'start': '"F{}"'.format(i+1), 'next': '', 'mem': mem, 'duration': time}
    output += generate_single_serial(last_arg_dict, serial_single)
    output = output[:-2] #delete ','
    output += generate_serial_end()
    return output
        
def main():
    import argparse
    global basic_serial_header
    global single_serial
    parser = argparse.ArgumentParser(description='')
    parser.add_argument('-s', '--nstages', dest='nstages', action='store',
                      help='Number of stages of chain', default='4')
    parser.add_argument('-m', '--mem', dest='mem', action='store',
                      help='Memory for a single function (currently homogeneous)', default=170)
    parser.add_argument('-t', '--time', dest='time', action='store',
                      help='Time spent on a single function in ms (currently homogeneous)', default=1000)
    parser.add_argument('-o', '--output', dest='output', action='store',
                      help='File to store output', default='workloads/dags/test')
    opts = parser.parse_args()
    
    output = (generate_chain(opts.nstages, opts.mem, opts.time, basic_serial_header, single_serial))
    opts.output += '_s{}_m{}_t{}.json'.format(opts.nstages, opts.mem, opts.time)
    f = open(opts.output, "w")
    f.write(output)
    f.close()
    
if __name__ == "__main__":
    main()
    




import argparse,sys
import glob,os
try:
    import simplejson as json
except ImportError:
    import json


def get_parser():
    parser = argparse.ArgumentParser('Load patients datasets from a JSON file obtained from mongoexport and convert it to datasets_loader format')
    parser.add_argument('--input_file', type=str, required=True,
                        help='The file from mongoexport in JSON format')
    parser.add_argument('--output_file', type=str, required=True,
                        help='The file in datasets_loader format')
    return parser


def main(argv):
    parser = get_parser()
    args = parser.parse_args(argv)
    #distribute ehr_records on patients files
    with open(args.input_file) as f:
        for line in f:
            jsoninput=json.loads(line)
            with open(jsoninput['patient_id'],'a') as g:
                g.write(json.dumps(jsoninput))
                g.write("\n")

    #create new dataset reading from patients files
    out=open(args.output_file,'w')
    ntot=len(glob.glob('PATIENT*'))
    for i,filename in enumerate(glob.glob('PATIENT*')):
        print str(i+1)+"/"+str(ntot)+" patients"
        with open(filename) as f:
            newline={}
            newlist=[]
            for j,line in enumerate(f):
                jsoninput=json.loads(line)
                if j==0:
                    patient_id=jsoninput['patient_id']
                itemlist=[4,jsoninput['ehr_data']]
                newlist.append(itemlist)
            newline[patient_id]=newlist
            out.write(json.dumps(newline))
            out.write("\n")
        os.remove(filename)

if __name__ == '__main__':
    main(sys.argv[1:])


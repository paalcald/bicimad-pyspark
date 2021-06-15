import json
from datetime import datetime
from pprint import pprint
from pyspark import SparkContext
import functools
import os

def pathInToOut(path, description):
    file_name = path.split("/")[-1]
    wd = os.getcwd()
    ofile_path_json = wd + "/data/" + file_name[:4] + "/" + file_name[4:]
    ofile_path_txt = ofile_path_json.replace(".json", ".txt").replace("movements", description)
    return (file_name[:4], ofile_path_txt)
def getSTD(x):
    try:
        std = ((x['idunplug_station'],
                x['idplug_station']),
                datetime.strptime(x['unplug_hourTime']['$date'],"%Y-%m-%dT%H:%M:%S.%f%z"))
    except:
        std = ((x['idunplug_station'],
                x['idplug_station']),
                datetime.strptime(x['unplug_hourTime'],"%Y-%m-%dT%H:%M:%S.%f%z"))
    return std
def getDeficit(rdd):
    deficit = rdd.mapValues(lambda xs: [x[1] for x in xs])\
                 .mapValues(lambda xs: functools.reduce(lambda x,y: x + y, xs))
    return sorted(deficit.collect())
def rawToStd(sc, ifile, ofile):
    rdd_base = sc.textFile(ifile)
    bicis = rdd_base.map(lambda x: json.loads(x))
    viajes = bicis.map(getSTD)\
                  .filter(lambda x: x[0][0] != x[0][1] and x[1].weekday() < 6)
    viajes_por_franja = []
    horas = [0,6,9,13,15,18,21,24]
    intervalos = zip(horas[:-1], horas[1:])
    for i, period in enumerate(intervalos):
        rdd = viajes.filter(lambda x: period[0] < x[1].hour and x[1].hour < period[1])\
                    .groupByKey()\
                    .mapValues(lambda x: len(x))\
                    .flatMap(lambda x: [x, ((x[0][1], x[0][0]), -x[1])])\
                    .map(lambda x: ((x[0][0],i), (x[0][1],x[1])))\
                    .groupByKey()\
                    .mapValues(list)
        viajes_por_franja.append(rdd)
    deficit_por_franja = list(map(getDeficit, viajes_por_franja))
    to_print = [f"{dato[0]}\t{dato[1]}\t{deficit}"
            for franja in deficit_por_franja
            for dato, deficit in franja]
    outfile = open(ofile, "w")
    for line in to_print:
        outfile.write(line + '\n')
    outfile.close()

if __name__ == '__main__':
    sc = SparkContext()
    hadoop = sc._jvm.org.apache.hadoop
    fs = hadoop.fs.FileSystem
    conf = hadoop.conf.Configuration()
    path = hadoop.fs.Path('/public_data/bicimad')
    cwd = os.getcwd()
    filenames = [str(f.getPath()) for f in fs.get(conf).listStatus(path)]
    date_files = [(f, pathInToOut(f, "deficit")) for f in filenames if 'stations' not in f]
    if not os.path.isdir("data"):
        os.mkdir("data")
        print("Created /data directory in wd")
    for date_file in date_files:
        ifile = date_file[0]
        ofile = date_file[1][0]
        year_dir = "data/" + date_file[1][0]
        if not os.path.isdir(year_dir):
            os.mkdir(year_dir)
            print("Created ", year_dir, " for that year's data archive")
        print("Working on ", ifile, " to store in ", date_file[1][1])
        rawToStd(sc, ifile, ofile)
        print("Done!")

from pyspark import SparkContext
from operator import add
import sys

def MapperOneMatA(Ipline):
    line = Ipline.split(",")
    return (line[1], "A" + "," + line[0] + "," + line[2])


def MapperOneMatB(Ipline):
    line = Ipline.split(",")
    return (line[0], "B" + "," + line[1] + "," + line[2])


def ReducerOne(MapperOneOP):
    Key = MapperOneOP[0]
    Value = MapperOneOP[1]
    LA = []
    LB = []
    for v in Value:
	val = v.split(",")
        if val[0] == "A":
            LA.append([val[1], int(val[2])])
        else:
            LB.append([val[1], int(val[2])])
    FinalList = []
    for a in LA:
        i = a[0]
        Aik = a[1]
        for b in LB:
            j = b[0]
            Bkj = b[1]
            FinalList.append([i + "," + j, Aik*Bkj])

    return FinalList


sc = SparkContext(appName="TwoPhaseMatMult")
A = sc.textFile(sys.argv[1])
B = sc.textFile(sys.argv[2])

pA = A.map(MapperOneMatA)	#Phase1 Mapper for matrix A
pB = B.map(MapperOneMatB)	#Phase1 Mapper for matrix B
pAB = pA.union(pB)		#Final output of Phase1 Mapper

ReducerOneOP = pAB.groupByKey().flatMap(ReducerOne)	#Phase1 Reducer
ReducerTwoOP = ReducerOneOP.reduceByKey(add).collect()	#Phase2 Reducer because Phase2 Mapper does nothing

fo = open(sys.argv[3],"w")
for v in ReducerTwoOP:
	fo.write(v[0] + "\t" + str(v[1]) + "\n")

fo.close()

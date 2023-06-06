from pyspark import SparkContext
# from sys import stdin

sc = SparkContext("local[*]", "accumulator")
sc.setLogLevel("ERROR")

# find number of blank/empty lines
if __name__ == "__main__":

    my_list = ["WARN: Tuesday 4 September 0405", "ERROR: Tuesday 4 September 0408", "ERROR: Tuesday 4 September 0408",
               "ERROR: Tuesday 4 September 0408", "ERROR: Tuesday 4 September 0408", "ERROR: Tuesday 4 September 0408"]

    rdd1 = sc.parallelize(my_list)

else:
    rdd1 = sc.textFile("E:/Big Data By Sumit Mittal/Week 10/Datasets/bigLogtxt-201014-183159/bigLog.txt")


rdd2 = rdd1.map(lambda x: (x.split(":")[0], 1))

rdd3 = rdd2.reduceByKey(lambda x, y: x+y)

result = rdd3.collect()

for a in result:
    print(a)

# else:
#     print("Not executed directly")

# stdin.readline()

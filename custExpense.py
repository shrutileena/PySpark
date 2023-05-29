from pyspark import SparkContext
# from sys import stdin

if __name__ == "__main__":
    sc = SparkContext("local[*]", "custexpense")
    sc.setLogLevel("ERROR")
    rdd1 = sc.textFile("E:/Big Data By Sumit Mittal/Week 9/Datasets/customerorders-201008-180523.csv")

    rdd2 = rdd1.map(lambda x: (x.split(",")[0], float(x.split(",")[2])))

    rdd3 = rdd2.reduceByKey(lambda x, y: x + y)

    rdd4 = rdd3.sortBy(lambda x: x[1], False)

    result = rdd4.collect()

    for a in result:
        print(a)

else:
    print("Not executed directly")

# stdin.readline()

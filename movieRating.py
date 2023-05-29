from pyspark import SparkContext
# from sys import stdin

if __name__ == "__main__":
    sc = SparkContext("local[*]", "moviedata")
    sc.setLogLevel("ERROR")
    lines = sc.textFile("E:/Big Data By Sumit Mittal/Week 9/Datasets/moviedata-201008-180523.data")

    ratings = lines.map(lambda x: (x.split("\t")[2],1))

    result = ratings.reduceByKey(lambda x, y: x+y).sortByKey(False).collect()

    for a in result:
        print(a)

else:
    print("Not executed directly")

# stdin.readline()

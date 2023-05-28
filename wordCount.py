from pyspark import SparkContext
from sys import stdin

if __name__ == "__main__":
    sc = SparkContext("local[*]", "wordcount")
    sc.setLogLevel("ERROR")
    inputData = sc.textFile("E:/Big Data By Sumit Mittal/Week 9/Datasets/search_data-201008-180523.txt")

    words = inputData.flatMap(lambda x: x.split(" "))

    word_counts = words.map(lambda x: (x, 1))

    final_count = word_counts.reduceByKey(lambda x, y: x + y)

    result = final_count.collect()

    for a in result:
        print(a)

else:
    print("Not executed directly")

stdin.readline()

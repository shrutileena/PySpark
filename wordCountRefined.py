from pyspark import SparkContext
# from sys import stdin

if __name__ == "__main__":
    sc = SparkContext("local[*]", "wordcount")
    sc.setLogLevel("ERROR")
    inputData = sc.textFile("E:/Big Data By Sumit Mittal/Week 9/Datasets/search_data-201008-180523.txt")

    words = inputData.flatMap(lambda x: x.split(" "))

    # lowercase
    word_counts = words.map(lambda x: (x.lower()))

    # countByValue
    final_count = word_counts.countByValue()

    print(final_count)

else:
    print("Not executed directly")

# stdin.readline()

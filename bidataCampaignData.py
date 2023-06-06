from pyspark import SparkContext
# from sys import stdin

def loadBoringWords():
    boring_words = set(line.strip() for line in open("E:/Big Data By Sumit Mittal/Week 10/Datasets/boringwords-201014-183159.txt"))
    return boring_words

if __name__ == "__main__":
    sc = SparkContext("local[*]", "campaignData")
    sc.setLogLevel("ERROR")

    name_set = sc.broadcast(loadBoringWords())

    lines = sc.textFile("E:/Big Data By Sumit Mittal/Week 10/Datasets/bigdatacampaigndata-201014-183159.csv")

    mapped_input = lines.map(lambda x: (float(x.split(",")[10]), x.split(",")[0]))

    words = mapped_input.flatMapValues(lambda x: x.split(" "))

    words_lower = words.map(lambda x: (x[1].lower(), x[0]))

    filtered_rdd = words_lower.filter(lambda x: x[0] not in name_set.value)

    final_map = filtered_rdd.reduceByKey(lambda x, y: x+y)

    sorted = final_map.sortBy(lambda x: x[1], False)

    result = sorted.take(20)

    for a in result:
        print(a)

else:
    print("Not executed directly")

# stdin.readline()

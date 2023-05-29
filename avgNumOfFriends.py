from pyspark import SparkContext


# from sys import stdin

def parse_line(line):
    fields = line.split("::")
    age = int(fields[2])
    num_of_friends = int(fields[3])
    return (age, num_of_friends)


if __name__ == "__main__":
    sc = SparkContext("local[*]", "friendsbyage")
    sc.setLogLevel("ERROR")
    friends_data = sc.textFile("E:/Big Data By Sumit Mittal/Week 9/Datasets/friendsdata-201008-180523.csv")

    rdd = friends_data.map(parse_line)

    # input (33, 385)
    # output (33, (385, 1))
    total_by_age = rdd.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1]))

    # input (33,(3000,5))
    # output 3000 / 5
    avg_by_age = total_by_age.mapValues(lambda x: x[0]/x[1])

    result = avg_by_age.collect()

    for a in result:
        print(a)

else:
    print("Not executed directly")

# stdin.readline()

from pyspark import SparkContext
# from sys import stdin

def black_line_checker(line):
    if(len(line) == 0):
        my_accumulator.add(1)

# find number of blank/empty lines
if __name__ == "__main__":
    sc = SparkContext("local[*]", "accumulator")
    sc.setLogLevel("ERROR")
    rdd1 = sc.textFile("E:/Big Data By Sumit Mittal/Week 10/Datasets/samplefile-201014-183159.txt")

    my_accumulator = sc.accumulator(0)

    rdd1.foreach(black_line_checker)

    print(my_accumulator.value)

else:
    print("Not executed directly")

# stdin.readline()

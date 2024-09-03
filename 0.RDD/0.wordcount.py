from pyspark import SparkContext, SparkConf

conf = SparkConf().setMaster('yarn')

sc = SparkContext(conf=conf) # 인스턴스화

# 로컬파일 읽기
# file_path = '/home/ubuntu/dmf/spark/0.RDD/input.txt'
# lines = sc.textFile(file_path)

# HDFS에서 파일 읽기
file_path = 'hdfs://localhost:9000/user/ubuntu/input/input.text'
lines = sc.textFile(file_path)


words = lines.flatMap(lambda line: line.split())
# print(words.collect())

mapped_words = words.map(lambda word: (word, 1))
# print(mapped_words.collect())

reduced_words = mapped_words.reduceByKey(lambda a, b: a+b)
# print(reduced_words.collect())

sc.stop()
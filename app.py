from pyspark import SparkConf, SparkContext
from pyspark.sql import Window
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

from itertools import combinations
import json

FREQ_DICT_FILENAME = 'freq_dict.json'
INPUT_FILE = 'input/puzzle3.txt'

def init_spark():
	sc=SparkContext('local[4]', appName = 'Jumble Solver')
	sc.setLogLevel("ERROR")
	spark = SparkSession.builder \
		.master("local") \
		.appName("Jumble Solver") \
		.getOrCreate()
	return spark

def get_most_freq_word(dic,jword):
	mfw = dic.filter(dic.sorted_word ==jword).first()
	if (mfw != '') and (mfw != None):
		if mfw.freq>0:
			v = 9888-mfw.freq
		else:
			v = mfw.freq
		return mfw.word, v
	else:
		return '', 0

def sentence_search(df, remain_chars, nch_list, res, score, final_list, total_score):
	if (len(nch_list) == 1) & (nch_list[0] == len(remain_chars)):
		jword = ''.join(sorted(remain_chars))

		mfw, v = get_most_freq_word(df,jword)
		if mfw != '':
			res.append(mfw)
			score.append(v)
			final_cand = ' '.join(res)
			if final_cand not in final_list:
				final_list.append(final_cand)
				ts = 0
				for s in score:
					ts += s
				total_score.append(ts)

			if len(res)>1:
				res.pop()
				res.pop()
				score.pop()
				score.pop()
		else:
			res.pop()
			score.pop()
		return res, final_list, total_score
	else:
		cc = combinations(remain_chars,nch_list[0])
		for c in cc:
			jword = ''.join(sorted(c))
			mfw, v = get_most_freq_word(df,jword)
			if mfw != '':
				tmp = remain_chars
				for ch in jword:
					pos = tmp.index(ch)
					tmp = tmp[:pos] + tmp[(pos+1):]

				res.append(mfw)
				score.append(v)

				res, final_list, total_score = sentence_search(df, tmp, nch_list[1:], res, score, final_list, total_score)
		if res != []:
			res.pop()
			score.pop()
		return res, final_list, total_score

def main():
	# initialize spark session
	spark = init_spark()

	# read frequency dictionary
	json_data=open(FREQ_DICT_FILENAME).read()
	data = json.loads(json_data)

	# make RDD
	schema = StructType([
		StructField('word', StringType(), False),
		StructField('freq', IntegerType(), False)])
	df = spark.createDataFrame(data.items(),schema)

	# sort the word and add column
	sort_word = udf(lambda chars:''.join(sorted(list(chars))), StringType())
	df = df.withColumn('sorted_word', sort_word(df.word))
	
	# group by the sorted word and take the most frequent word
	window = (Window
          .partitionBy(df['sorted_word'])
          .orderBy(df['freq'].desc()))


	df_max = (df
	 .select('*', rank()
	         .over(window)
	         .alias('rank')) 
	  .filter(col('rank') == 1)
	  .orderBy(col('freq'))
	  .dropDuplicates(['sorted_word'])
	  .drop('rank')
	)

	# df_max.cache()
	df_max.persist()

	# read puzzle
	inpfile = open(INPUT_FILE)
	anagram = eval(inpfile.readline().replace('\n',''))
	sentence_length = eval(inpfile.readline().replace('\n',''))

	# solve the jumble (part1)
	chars_for_sentence = []
	for i, v in anagram.items():
		jword = ''.join(sorted(i.lower()))

		most_frequent_word, freq_v = get_most_freq_word(df_max,jword)
		print i, '\t|', most_frequent_word

		chars_for_sentence.extend([most_frequent_word[i] for i, x in enumerate(v) if x == "1"])

	chars_for_sentence = ''.join(chars_for_sentence)

	print chars_for_sentence

	# solve the jumble (part2 - sentence)
 	final_list = []
	total_score = []
	res = []
	score = []

	res, final_list, total_score = sentence_search(df_max, chars_for_sentence, sentence_length, res, score, final_list, total_score)

	idlist = sorted(range(len(total_score)), key=lambda k: total_score[k], reverse=True)
	
	maxN = 10
	for i in idlist[0:maxN]:
		print total_score[i], final_list[i]

if __name__ == '__main__':
    main()

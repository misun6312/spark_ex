from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

from itertools import combinations
import json

FREQ_DICT_FILENAME = 'freq_dict.json'
INPUT_FILE = 'puzzle4.txt'

def init_spark():
	sc=SparkContext('local[4]', appName = 'Jumble Solver')
	sc.setLogLevel("ERROR")
	spark = SparkSession.builder \
		.master("local") \
		.appName("Jumble Solver") \
		.getOrCreate()
	return spark

def get_most_freq_word(dic,jword):
	mfw = dic.where(col('sorted_word') == jword).orderBy('freq',ascending=False).first()
	if (mfw != '') and (mfw != None):
		return mfw.word
	else:
		return ''

def sentence_search(df, remain_chars, nch_list, res, score, final_list, total_score):
	if (len(nch_list) == 1) & (nch_list[0] == len(remain_chars)):
		jword = ''.join(sorted(remain_chars))

		mfw = get_most_freq_word(df,jword)
		if mfw != '':
			v = df.where(col('word')==mfw).first().freq
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
			mfw = get_most_freq_word(df,jword)
			if mfw != '':
				tmp = remain_chars
				for ch in jword:
					pos = tmp.index(ch)
					tmp = tmp[:pos] + tmp[(pos+1):]
				v = df.where(col('word')==mfw).first().freq

				res.append(mfw)
				score.append(v)

				res, final_list, total_score = sentence_search(df, tmp, nch_list[1:], res, score, final_list, total_score)
		if res != []:
			res.pop()
			score.pop()
		return res, final_list, total_score

def main():
	spark = init_spark()

	json_data=open(FREQ_DICT_FILENAME).read()
	data = json.loads(json_data)

	schema = StructType([
		StructField('word', StringType(), False),
		StructField('freq', IntegerType(), False)])
	df = spark.createDataFrame(data.items(),schema)

	sort_word = udf(lambda chars:''.join(sorted(list(chars))), StringType())
	df = df.withColumn('sorted_word', sort_word(df.word))
	df.cache()

	# read puzzle
	inpfile = open(INPUT_FILE)
	anagram = eval(inpfile.readline().replace('\n',''))
	sentence_length = eval(inpfile.readline().replace('\n',''))

	chars_for_sentence = []
	for i, v in anagram.items():
		jword = ''.join(sorted(i.lower()))

		most_frequent_word = get_most_freq_word(df,jword)
		print i, '\t|', most_frequent_word

		chars_for_sentence.extend([most_frequent_word[i] for i, x in enumerate(v) if x == "1"])

	chars_for_sentence = ''.join(chars_for_sentence)

	print chars_for_sentence

 	final_list = []
	total_score = []
	res = []
	score = []

	res, final_list, total_score = sentence_search(df, chars_for_sentence, sentence_length, res, score, final_list, total_score)

	idlist = sorted(range(len(total_score)), key=lambda k: total_score[k], reverse=True)
	
	maxN = 10
	for i in idlist[0:maxN]:
		print total_score[i], final_list[i]

if __name__ == '__main__':
    main()

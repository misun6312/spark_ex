from itertools import combinations
import json

def get_possible_word(dict, jword):
	if jword in dict:
		return dict[jword]
	else:
		return ''

def recur_search(dict, remain_chars, nch_list, res, score):
	if (len(nch_list) == 1) & (nch_list[0] == len(remain_chars)):
		jword = ''.join(sorted(remain_chars))
		pws = get_possible_word(dict,jword)
		if pws != '':
			most_frequent_word = max(pws,key=pws.get)
			v = pws[most_frequent_word]
			res.append(most_frequent_word)
			score.append(v)

			# final_list[cnt] = res
			# cnt += 1
			final_list.append(' '.join(res))
			total_score.append(sum(score))
			# print res
			# final_list[res] = sum(score)
			if len(res)>1:
				res.pop()
				res.pop()
				score.pop()
				score.pop()
		else:
			res.pop()
			score.pop()
		return res
	else:
		cc = combinations(remain_chars,nch_list[0])
		for c in cc:
			jword = ''.join(sorted(c))
			pws = get_possible_word(dict,jword) 
			if pws != '':
				tmp = remain_chars
				for ch in jword:
					pos = tmp.index(ch)
					tmp = tmp[:pos] + tmp[(pos+1):]
				most_frequent_word = max(pws,key=pws.get)
				v = pws[most_frequent_word]
				# print remain_chars, ' - ', jword, ' => ', tmp
				res.append(most_frequent_word)
				score.append(v)

				res = recur_search(dict, tmp, nch_list[1:], res, score)
		if res != []:
			res.pop()
			score.pop()
		return res


def main():
	json_data=open('freq_dict.json').read()
	d = json.loads(json_data)

	dict = {}
	for word, v in d.items():
		sorted_word = ''.join(sorted(word))
		# dict[sorted_word][word] = v
		if sorted_word in dict:
			dict[sorted_word][word] = v
		else:
			dict[sorted_word] = {}
			dict[sorted_word][word] = v

	#find jumbles
	inp = {'NAGLD':'01011','RAMOJ':'00110','CAMBLE':'110100','WRALEY':'101010'}
	inp2 = [3,4,4]

	# inp = {'BNEDL':'10001','IDOVA':'10011','SEHEYC':'010001','ARACEM':'010011'}
	# inp2 = [3,4,3]
	
	# inp = {'SHAST':'10011','DOORE':'11010','DITNIC':'111000','CATILI':'101001'}
	# inp2 = [4, 8]
	
	# inp = {'KNIDY':'11000','LEGIA':'10100','CRONEE':'010100','TUVEDO':'100001'}
	# inp2 = [8]
	
	# inp = {'GYRINT':'110100','DRIVET':'001001','SNAMEA':'100001','CEEDIT':'010101','SOWDAH':'100100','ELCHEK':'010001'}
	# inp2 = [6, 8]

	chars = []
	for i, v in inp.items():
		jword = ''.join(sorted(i.lower()))
		pws = get_possible_word(dict,jword)
		most_frequent_word = max(pws,key=pws.get)
		print i, most_frequent_word

		chars.extend([most_frequent_word[i] for i, x in enumerate(v) if x == "1"])

	main_chars = ''.join(chars)

	print main_chars

	# perm = permutations('lwejolndbea',inp2[0])
	# perm = permutations('lwejolndbea',inp2[1])
	# perm = permutations('lwejolndbea',inp2[2])
	# => total score => max score


	global final_list
	global total_score

 	final_list = []
	total_score = []

	res = []
	score = []
	res = recur_search(dict, main_chars, inp2, res, score)

	idlist = sorted(range(len(total_score)), key=lambda k: total_score[k], reverse=True)
	
	# idd = total_score.index(max(total_score))
	# print(max(total_score), final_list[idd])

	maxN = 20
	for i in idlist[0:maxN]:
		print total_score[i], final_list[i]


main()

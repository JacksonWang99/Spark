
# Build the function to calculate counter
#The function of this function is: dataHashCode, queryHashCode, which is the 
#initial value of the offset data 0,1,2,3 to continuously relax the conditions
# q   1 1 1  1  1 1 1  1 1  1
# o1  1 0 2 -1  1 2 4 -3 0 -1
#Check whether the corresponding positions of the two sets of data can be matched, 
#and how to match the difference between the upper and lower values <= offset 
#indicates that the conditions are met
def collision_count(a, b, offset):
	counter = 0
	for i in range(len(a)):
		#Start to match the value of the corresponding position
		# if the difference is less than offset, then counter+=1
		if abs(a[i]-b[i]) <= offset:
			counter += 1
	#got coungter
	return counter

def c2lsh(data_hashes, query_hashes, alpha_m, beta_n):
	#initialize offset and cand_num
	offset = 0
	cand_num = 0
	#do while loop 
	while cand_num < beta_n :
		# got condidates 
		# [x[0]] if collision_count(x[1], query_hashes, offset)>=alpha_m else [] 
		#if counter >= the value alpha_m,need to stop 
		##using flatMap method
		candidates = data_hashes.flatMap(lambda x :[x[0]] if collision_count(x[1], 
			query_hashes, offset)>=alpha_m else [])
		# do count operation
		cand_num = candidates.count()
		# if cand_num != beta_n, we need to relax offset
		offset += 1
	return candidates
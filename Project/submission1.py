## import modules here

########## Question 1 ##########
# do not change the heading of the function
def c2lsh(data_hashes, query_hashes, alpha_m, beta_n):
    offset = 0
    Num_candidate = 0
    while Num_candidate < beta_n:
        RDD = data_hashes.flatMap( lambda e : e[0] if (count(e[1],query_hashes,alpha_m,offset)==1) else [])
        offset += 1
        Num_candidate = RDD.count()
    return RDD

def count(data_hashes,query_hashes,alpha_m,offset):
    count = 0
    l = len(data_hashes)
    for i in range(l):
        if (abs(data_hashes[i] - query_hashes[i]) <= offset):
            count += 1
    return bool(count>=alpha_m)
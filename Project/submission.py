## import modules here

def c2lsh(data_hashes, query_hashes, alpha_m, beta_n):
    offset = -1
    num_rdd_condidates = -1
    while num_rdd_condidates < beta_n:
        offset += 1
        rdd_condidates = data_hashes.flatMap(lambda e: [e[0]] if checkmatch(e[1], query_hashes, alpha_m, offset) else [])
        num_rdd_condidates = rdd_condidates.count()
    return rdd_condidates


def checkmatch(data_hashes, query_hashes, alpha_m, offset):
    counter = 0
    for i in range(len(data_hashes)):
        if ( abs(data_hashes[i] - query_hashes[i]) <= offset):
            counter += 1
            if counter >= alpha_m:
                return counter
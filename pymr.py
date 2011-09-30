## http://github.com/sabirribas/pymr Join us! :)

# Basic MapReduce

def mapreduce( fmap , freduce , inputlist ):
	mapped = map ( fmap , inputlist )
	toreduce = {}
	for kvl in mapped:
		for (k,v) in kvl:
			toreduce[k]=[]
	for kvl in mapped:
		for (k,v) in kvl:
			toreduce[k].append(v)
	reduced = map ( lambda k: ( k , freduce(toreduce[k]) ) , toreduce )
	return reduced

# Parallel MapReduce based on Pool

from multiprocessing import Pool

def preduce((x,freduce,toreducex)):
	return ( x , freduce(toreducex) )

# Restriction: fmap and freduce can not be a lambda function!

def pmapreduce( fmap , freduce , inputlist ):
	pool = Pool(processes=len(inputlist))
	mapped = pool.map ( fmap , inputlist )
	toreduce = {}
	for kvl in mapped:
		for (k,v) in kvl:
			toreduce[k]=[]
	for kvl in mapped:
		for (k,v) in kvl:
			toreduce[k].append(v)
	pool = Pool(processes=len(toreduce))
	topreduce = map ( lambda x : (x,freduce,toreduce[x]) , toreduce )
	reduced = pool.map ( preduce , topreduce )
	return reduced


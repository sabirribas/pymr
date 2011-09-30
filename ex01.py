# Example 1: Word counting

import pymr
import time

def fmap(x):
	time.sleep(1)
	print "map"
	return [x]

def freduce(vs):
	time.sleep(1)
	print "reduce"
	return sum(vs)

if __name__ == '__main__':
	print pymr.pmapreduce( fmap , freduce , [('a',1),('b',1),('a',1),('a',1)] )	
	print pymr.mapreduce( fmap , freduce , [('a',1),('b',1),('a',1),('a',1)] )

# Example 2: Word counting

import pymr
import sys

def fmap((filename,nothing)):
	f = open(filename,'r')
	text = f.read()
	f.close()
	stext = text.replace('\n',' ').replace(',',' ').split(' ')
	return zip( stext , [1 for i in range(0,len(stext))] )

def freduce(vs):
	return sum(vs) # same as reduce(lambda v1,v2:v1+v2,vs)

if __name__ == '__main__':
	if len(sys.argv) != 2:
		print 'Usage: python ex02.py <filename>'
	else:
		print pymr.pmapreduce( fmap , freduce , [(sys.argv[1],-1)] )

import os
import random
from random import choice
zeroone=['1','0']
output=open('data','w+')
for i in range(1,100):
	output.write(choice(zeroone))
	for j in range(1,100):
		output.write(' ')
		output.write(str(random.random()))
	output.write('\n')
output.close()

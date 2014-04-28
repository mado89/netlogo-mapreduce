from random import random
from random import randint
import sys

with open(sys.argv[1],"w") as f:
	for i in range(0,400):
		# f.write("%f %f\n" % (round(random()*20,2),round(random()*2,2)))
		f.write("%d %d\n" % (randint(0,200), randint(0,200)))


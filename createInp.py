import random

out= {}
strings= []
chars= ['a','b','c','d','e','f','g','h','i','j','k','l']
counts= {}

def createFiles(count):
	for i in range(0,count):
		out[i]= open("../input/" + str(i) + ".txt", "w")

def write(count, text):
	o= out[random.randint(0, count-1)]
	o.write(text + "\n")
	counts[text]= counts[text] + 1
	
def closeFiles(count):
	for i in range(0,count):
		out[i].close()
		
def buildStrings(count):
	for x in range(0,count):
		l= random.randint(2,15)
		s= ""
		for i in range(0,l):
			s+= chars[random.randint(0,len(chars)-1)]
		strings.append(s)
		counts[s]= 0

createFiles(5)
buildStrings(5)
for string in strings:
	for i in range(1, random.randint(5,20)):
		write(5,string)
closeFiles(5)
for x in counts:
	print "" + x + ": " + str(counts[x])

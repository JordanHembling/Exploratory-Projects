file = open('test.txt')

# print(file.read(2)) # read bytes 
# print(file.readline()) # read line
# print(file.readline())  # read next line (remembers state)

## print line by line 
# line = file.readline() 
# while line !=  "":
#     print(line)
#     line = file.readline() 

## read lines 
## readlines stores each line into a list 
lines = file.readlines()

[print(line) for line in lines]

file.close()
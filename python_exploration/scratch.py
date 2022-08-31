# values = [1, 2, 'tree', 3, 'blue']

# #print(values[1:3])

# values[2] = 'bush'

# #print(values)


# ## create dict at run time 

# first_names = ["Rahul", "Mandy", "Alex"]
# last_names = ["Deepak" , "Tris", "Jones"]
# genders = ["M", "F", "M"]

# my_list = []
# for i in range(0, len(first_names)):
#     obj = {}
#     obj["firstname"] = first_names[i]
#     obj["lastname"] = last_names[i]
#     obj["gender"] = genders[i]

#     my_list.append(obj)
#     i+=1

# print(my_list)


# def my_func():
#     print("I am returning 1")
#     yield 1

#     print("I am returning 2")
#     yield 2

#     print("I am returning 3")
#     yield 3  

# gen = my_func()

# gen

# # next(gen)
# # next(gen)
# # next(gen)

# for val in my_func(): # for loop understands next and handles stop iteration 
#     print(val)


def my_func():
    for val in range(100):
        yield val 


for v in my_func():
    print(v)
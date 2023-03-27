# from solutions import Solution

# solution = Solution() #instantiating a class
# # solution.move_zeros([0,0,4,6,2,0,2,0]) #calling a method from the class instantieded

# solution.boats_to_save([3,2,1,2,2,1],4)



#================== iterator ==================#

my_list = [1,2,3,4]
my_iterator = iter(my_list)

# print(next(my_iterator))
# print(next(my_iterator))
# print(next(my_iterator))
# print(next(my_iterator))
# print(next(my_iterator))

# for i in my_iterator:
#     print(i)

#================== generator ==================#
def my_generator(n):
    value = 0
    while value < n:
        yield value
        value += 1

# iterate over the generator object produced by my_generator
for value in my_generator(3):
    print(value)





# def calc(n):
#     return n*n

# result = map(calc,[1,2,3])
# print(result)

# for i in result:
#     print(i)

# def extendList(val, list=[]):
#     list.append(val)
#     return list

# list1 = extendList(10)
# list2 = extendList(123,[])
# list3 = extendList('a')
# print(list1)
# print("list1 = %s" % list1)
# print("list2 = %s" % list2)
# print("list3 = %s" % list3)
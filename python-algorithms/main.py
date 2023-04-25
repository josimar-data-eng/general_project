from solutions import Solution

solution = Solution() #instantiating a class

# print(solution.reverse_integer(-123))
# print(solution.add_strings("123","1"))
# solution.move_zeros([0,0,4,6,2,0,2,0,2])
# solution.boats_to_save([10,10,20,30,30],30)
# print(solution.valid_palindrome("radkkalr"))
# print(solution.monotonic_array([1,2,3,4,4]))
# print(solution.bubble_sort([0,0,4,6,2,10,2,-1,2]))
# print(solution.fill_blanks([1,2,None, None, 3, 4, None]))
# print(solution.match_mismatch("My name is JJ","My name are there"))
# print(solution.average_w_length("Hi all, my name is Tom...I am originally from Australia."))



# d = 3
# temp = []
# temp2 = []
# a = [1,2,3,4,5]

# print("original: ",a)

# print("shift to right")
# for i in range(len(a)-d,len(a)):
#     temp.append(a[i])
# temp.extend(a[0:len(a)-d])
# print(temp)

# print("shift to left")
# for i in range(d,len(a)):
#     temp2.append(a[i])
# temp2.extend(a[0:d])
# print(temp2)


# 1)ANAGRAM NERD WALLET
# Input: strs = ["eat","tea","tan","ate","nat","bat"]
# Output:       [["bat"],["nat","tan"],["ate","eat","tea"]] 
a = ["eat","tea","tan","ate","nat","bat"]
print(sorted(a[1]))
def anagram(anagram_list):
    list_output = []
    temp_list_output = []
    for i in anagram_list:
        for j in anagram_list:
            if (sorted(i)==sorted(j)): # and j not in temp_list_output):
                temp_list_output.append(j)        
        temp_list_output = list(set(temp_list_output))                
        if temp_list_output not in list_output:
            list_output.append(temp_list_output)
        temp_list_output = []
    return list_output
# print(anagram(["eat","tea","tan","ate","nat","bat"]))













#======== ITERATOR =========#

# my_list = [1,2,3,4]
# my_iterator = iter(my_list)

# # print(next(my_iterator))
# # print(next(my_iterator))
# # print(next(my_iterator))
# # print(next(my_iterator))
# # print(next(my_iterator))

# for i in my_iterator:
#     print(i)


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
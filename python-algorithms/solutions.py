from typing import List

class Solution:

    def move_zeros(self, array:List[int]):
        print("\nInitial array: ",array,"\n")
        print("==== Method 1 ====")
        move_left_array  = []
        move_right_array = []
        for i in array: # BigO(n) due to loop in each single element
            if i != 0:
                move_left_array.append(i)
                move_right_array.append(i)
        for i in range(len(array)-len(move_left_array)):    #BigO(m) due to loop in part of the loop
            move_right_array.append(0)
            move_left_array.insert(0,0)
        print("move_left_array.: ",move_left_array)
        print("move_right_array: ",move_right_array,"\n")


        print("\n==== Method 2 ====\n")
        pointer=0
        for i in array:
            if i != 0:
                array[pointer]=i
                pointer+=1
        pointer_m3=pointer

        for i in array[pointer:]:
            array[pointer]=0
            pointer+=1
        print(array)

        print("\n==== Method 3 ====")
        for i in range(pointer_m3,len(array)):
            array[i]=0
        print(array)


    def boats_to_save(self,array:List[int],limit:int):
        array.sort()
        #method1
        print("\n==== Method 1 ====")
        sum_weight = sum(array)
        if sum_weight%limit == 0:
            print(f"Boats to save: {sum_weight//limit}")
        else:
            print(f"Boats to save: {(sum_weight//limit)+1}")

        #method2
        print("\n==== Method 2 ====")        
        boats=0
        lightest_person_index = 0
        heaviest_person_index = len(array)-1
        while heaviest_person_index >= lightest_person_index:
            if array[lightest_person_index]+array[heaviest_person_index] <= limit:
                heaviest_person_index-=1
                lightest_person_index+=1
            else:
                heaviest_person_index-=1
            
            boats+=1
        
        print(f"Boats to save: {boats}")



    def bubble_sort(self, array:List[int]):
        for i in range(len(array)):          # i:0     | i:1     | i:2
            for j in range(0,len(array)-i-1):# j:(0,4) | j:(0,3) |j:(0,2)
                if array[j] > array[j+1]:
                    array[j],array[j+1]=array[j+1],array[j]
        return array


    def reverse_integer(self, number:int) -> int:

        str_number = str(number)
        if str_number[0] == "-":
            number = int("-"+str_number[:0:-1])
        else:
            number = int(str_number[::-1])
        return number

    
    def average_w_length(self, string:str) -> int:

        p = ".,/?!-:;"
        for i in p:
            string = string.replace(i,'')
        words = string.split()
        return round(sum([len(word) for word in words])/len(words),2)


    def palindrome_leetcode(self, s:str) -> bool:
        symbols = ".,/?!-:; "
        for i in symbols:
            s.replace(i,'')
        if s.lower() == s.lower[::-1]:
            return True
        return False

    def valid_palindrome(self, s:str) -> bool:
        symbols = ".,/?!-:; "

        for i in symbols:
            s.replace(i,'')
        s = s.lower()
        for i in range(len(s)):
            t = s[:i]+s[i+1:]
            if t == t[::-1]:
                return True
        return s==s[::-1]

    def add_strings(self, n1:str, n2:str) -> str:
        return str(eval(n1)+eval(n2))


    def monotonic_array(self, array:List[int]) -> List[int]:

        #method 1
        ascending = 0
        descending = 0
        for i in range(len(array)-1):
            if array[i] <= array[i+1]:
                ascending+=1

        for i in range(len(array)-1):
            if array[i] <= array[i+1]:
                descending+=1

        # if ascending == len(array)-1 or descending == len(array)-1:
        #     return True

        #method2

        return (all([array[i] <= array[i+1] for i in range(len(array)-1)]) 
                or
                all([array[i] >= array[i+1] for i in range(len(array)-1)])
                )

    def fill_blanks(self, array:List[int]) -> List[int]:

        for i in range(len(array)):
            if array[i] == None:
                array[i] = array[i-1]
        
        return array


    def match_mismatch(self, s1:str,s2:str) -> List[int]:
        
        difference = []
        intesection = []

        #method1
        array1 = s1.replace(".,/?!-:;",'').split()
        array2 = s2.replace(".,/?!-:;",'').split()        

        for i in array1:

            if i not in array2:
                difference.append(i)

            for j in array2:
                if i ==j:
                    intesection.append(i)
                
                if j not in array1 and j not in difference:
                    difference.append(j)
    

        #method2
        # set_1 = set(s1.replace(".,/?!-:;",'').split())
        # set_2 = set(s2.replace(".,/?!-:;",'').split())

        # intesection = list(set_1.intersection(set_2))
        # difference  = list(set_1.symmetric_difference(set_2))

        return difference, intesection


    def valid_mountain(self, a:List[int]) -> List[int]:
        i=1
        while (i<len(a) and a[i]>a[i-1]):
            i+=1
        
        if i==1 or i==len(a):
            return False
        
        while (i< len(a) and a[i]<a[i-1]):
            i+=1

        return i==len(a)



#======================= SIMPLE METHODS =======================#


# REVERSE LOOP
#1
# for i in reversed(prime):
#     print(i)
#2
# for i in range(len(prime)-1,-1,-1):
#     print(prime[i])




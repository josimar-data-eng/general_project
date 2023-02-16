from typing import List

class Solution:

    def move_zeros(self, array:List[int]):
        #method1
        # new_array = []
        # for i in array:   BigO(n) due to loop in each single element
        #     if i != 0:
        #         new_array.append(i)
        # print(new_array)
        # for i in range(len(array)-len(new_array)):    BigO(m) due to loop in part of the loop
        #     new_array.append(0)
        # print(new_array)

        #method2
        # j=0
        # for i in array:
        #     if i != 0:
        #         array[j]=i
        #         j+=1
        # for i in array[j:]:
        #     array[j]=0
        #     j+=1
        # print(array)

        #method3
        j=0
        for i in array:
            if i != 0:
                array[j]=i
                j+=1
        for i in range(j,len(array)):
            array[i]=0
        print(array)


    def boats_to_save(self,array:List[int],limit:int):
        array.sort()
        #method1
        print("==== Method 1 ====")
        sum_weight = sum(array)
        if sum_weight%limit == 0:
            print(f"Boats to save: {sum_weight//limit}")
        else:
            print(f"Boats to save: {(sum_weight//limit)+1}")

        #method2
        print("\n==== Method 2 ====")        
        boats=0
        lightest_person_index = 0
        heavist_person_index  = len(array)-1
        while heavist_person_index > lightest_person_index:
            if array[lightest_person_index]+array[heavist_person_index] <= limit:
                heavist_person_index-=1
                lightest_person_index+=1
            else:
                heavist_person_index-=1
            
            boats+=1
        
        print(f"Boats to save: {boats}")

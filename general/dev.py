# print(2 + 3 * 5.)
# print( 1/2+3//3+4**2)
# print(4/0.5)
# print(2//4)

# x = 11 % 4
# x = x % 4
# y = 4 % x
# print(y)
# print(1//2*3)

test = [1,2,3]
_reversed = []
latest_index = test.index(len(test))


for i in reversed(range(latest_index+1)):
    _reversed.append(test[i])
    

print(test)
print(_reversed)
t = test.pop()
print(t)
print(test)
##1)Given a number count the total number of digits in a number.
n=int(input("Enter the number"))
t=n
cnt=0
while t!=0:
    t//=10
    cnt=cnt+1
print("Total number of digits in",n,"is",cnt)

##1b)Reverse the following list using for loop
list1 = [10, 20, 30, 40, 50]
list1.reverse()
for i in list1:
    print(i)

##2)Given 2 strings, s1 and s2, create a new string by appending s2 in the middle of s1
##Expected Outcome:
## appendMiddle("Chrisdem", IamNewString) → "ChrIamNewStringisdem"

def appendMiddle(s1,s2):
    n=len(s1)//2
    return(s1[:n-1]+s2+s1[n-1:])

s1=input("Enter the 1st string")
s2=input("Enter the 2nd string")
print(appendMiddle(s1,s2))

##3)Arrange String characters such that lowercase letters should come first
##Given input String of combination of the lower and upper case arrange characters in such a way that all
##lowercase letters should come first.
##Expected Output:
##input_String = PyNaTive
##arranging characters giving precedence to lowercase letters
##aeiNPTvy
string=input("Enter the string:")

List = string.split()
lower_char_list = []
upper_char_list = []
for char in string:
    if char.islower():
        lower_char_list.append(char)
    else:
        upper_char_list.append(char)
string1 = ''.join(lower_char_list + upper_char_list)
print("arranging characters giving precedence to lowercase letters:"+str(string1))


##4) Given a string, return the sum and average of the digits that appear in the string, 
##ignoring all other characters 
##For Example: –  sumAndAverage("English = 78 Science = 83 Math = 68 History = 65") = sum 294 Percentage is 73.5
import re

inputStr = "English = 78 Science = 83 Math = 68 History = 65"
markList = [int(num) for num in re.findall(r'\b\d+\b', inputStr)]

totalMarks = 0
for mark in markList:
  totalMarks+=mark

percentage = totalMarks/len(markList)  
print("Total Marks is:", totalMarks, "Percentage is ", percentage)
print(markList)

##5)Given a two list. Create a third list by picking an odd-index element from the first list and even index
##elements from second.
##For Example:
##listOne = [3, 6, 9, 12, 15, 18, 21]
##listTwo = [4, 8, 12, 16, 20, 24, 28]
## Expected Output:
##Element at odd-index positions from list one
##[6, 12, 18]
##Element at even-index positions from list two
##[4, 12, 20, 28]
##Printing Final third list
##[6, 12, 18, 4, 12, 20, 28]

n=int(input("Enter the length of list:"))
listOne=list(map(int,input("Enter listOne elements").strip().split()))[:n]
listTwo=list(map(int,input("Enter ListTwo elements").strip().split()))[:n]





odd_index_list=listOne[1::2]
even_index_list=listTwo[0::2]
print(odd_index_list)
print(even_index_list)
print(odd_index_list+even_index_list)

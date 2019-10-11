
import random
import string

def random_string(length=10):
    """Generate a random string of fixed length """
    letters = string.ascii_lowercase
    return ''.join(random.choice(letters) for i in range(length))

for i in range(0,10):
  f = open("test" + str(i) + ".csv","w+")
  f.write("col_a,col_b,col_c\n")
  for line in range(0,1000000):
    if line % 10 == 0:
      a = "aaa"
    else:
      a = random_string(random.randint(3,10))
    b = random_string(3)
    c = random_string(3)
    f.write(",".join([a,b,c]))
    f.write("\n")
  f.close()
  

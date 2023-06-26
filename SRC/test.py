import random
import time
for i in range(10):
    time.sleep(random.choices(range(1,10), k=1)[0])
    print(i)
    
    # print random.choices(range(1,10), k=1)[0]



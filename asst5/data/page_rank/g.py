import random

n = 10000

f = open("large.txt", 'a')

for i in range(1, n+1):
    line = str(i) + " " + str((i%n)+1) + "\n"
    f.write(line)

    node0 = random.randrange(1, n+1)
    if node0 == 1:
        continue
    else:
        node1 = random.randrange(1, node0)

        line = str(node0) + " " + str(node1)
        if i < n:
            line = line + "\n"

        f.write(line)

f.close()


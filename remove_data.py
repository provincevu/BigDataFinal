with open("data/online_retail.csv", "r") as f:
    with open("data/data.csv", "w") as j:
        for i in range(20000):
            line = f.readline()
            j.write(line)
        
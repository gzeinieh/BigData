import sys

print("Creating BFS starting input for charcter"+sys.argv[1])

with open("data/BFS-iteration-0.txt",'w') as out:
    with open("data/Marvel-Graph.txt") as f:
        for line in f:
            data = line.split()
            hero_id = data[0]
            connections = data[1:]
            distance = 9999
            color = "WHITE"

            if hero_id == sys.argv[1]:
                color = "GRAY"
                distance = 0

            edges = ",".join(connections)
            out_line = "|".join((hero_id,edges,str(distance),color))
            out.write(out_line)
            out.write("\n")

    f.close()

out.close()
if not exist classesPowerIteration mkdir classesPowerIteration
javac -cp . -d classesPowerIteration PageRank.java  
java -cp classesPowerIteration -Xmx1g PageRank linksDavis.txt
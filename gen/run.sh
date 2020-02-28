kafka-server-start /usr/local/Cellar/kafka/2.2.1/libexec/config/server.properties

zookeeper-server-start /usr/local/Cellar/kafka/2.2.1/libexec/config/zookeeper.properties

python3 botgen.py --file data.txt

flume-ng agent --conf conf --conf-file flume.conf  -Dflume.root.logger=DEBUG,console --name a1 -Xmx512m -Xms256m
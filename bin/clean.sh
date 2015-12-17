echo "Cleaning local log:"
echo > /home/tacitus/hadoop-0.21.0/bin/../logs/hadoop-tacitus-namenode-se2.log
echo "=========================================================================================="
echo "Stop DFS instance in the cluster"
./stop-dfs.sh 
echo "Stop MapReduce in the cluster"
./stop-mapred.sh 
echo ""

echo "=========================================================================================="
echo "Formatting HDFS:"
echo "Format HDFS on the local namenode:"
hadoop namenode -format
echo "Format HDFS on the netmonitor"
ssh tacitus@netmonitor "hadoop namenode -format"
echo "Format HDFS on the strong"
ssh tacitus@strong "hadoop namenode -format"

echo "Clearing HDFS data on local host"
rm -rf  /tmp/hadoop-tacitus/dfs/data/*
echo "Cleaning logs..."
echo > /home/tacitus/hadoop-0.21.0/bin/../logs/hadoop-tacitus-namenode-se2.log
echo > /home/tacitus/hadoop-0.21.0/bin/../logs/hadoop-tacitus-datanode-se2.log
echo > /home/tacitus/hadoop-0.21.0/bin/../logs/hadoop-tacitus-tasktracker-se2.log
echo > /home/tacitus/hadoop-0.21.0/bin/../logs/hadoop-tacitus-jobtracker-se2.log

echo "Clearing HDFS data on remote host[netmonitor]"
ssh tacitus@netmonitor "rm -rf /tmp/hadoop-tacitus/dfs/data/*"
echo "Cleaning logs..."
ssh tacitus@netmonitor "echo > /home/tacitus/hadoop-0.21.0/bin/../logs/hadoop-tacitus-namenode-NetMonitor.log"
ssh tacitus@netmonitor "echo > /home/tacitus/hadoop-0.21.0/bin/../logs/hadoop-tacitus-datanode-NetMonitor.log"
ssh tacitus@netmonitor "echo > /home/tacitus/hadoop-0.21.0/bin/../logs/hadoop-tacitus-tasktracker-NetMonitor.log"
ssh tacitus@netmonitor "echo > /home/tacitus/hadoop-0.21.0/bin/../logs/hadoop-tacitus-jobtracker-NetMonitor.log"


echo "Clearing HDFS data on remote host[strong]"
ssh tacitus@strong "rm -rf /tmp/hadoop-tacitus/dfs/data/*"
echo "Cleaning logs..."
ssh tacitus@strong "echo > /home/tacitus/hadoop-0.21.0/bin/../logs/hadoop-tacitus-namenode-strong.log"
ssh tacitus@strong "echo > /home/tacitus/hadoop-0.21.0/bin/../logs/hadoop-tacitus-datanode-strong.log"
ssh tacitus@strong "echo > /home/tacitus/hadoop-0.21.0/bin/../logs/hadoop-tacitus-tasktracker-strong.log"
ssh tacitus@strong "echo > /home/tacitus/hadoop-0.21.0/bin/../logs/hadoop-tacitus-jobtracker-strong.log"

/home/tacitus/hadoop-0.21.0/bin/stop-dfs.sh
echo "========================================================================================================"
echo "building HDFS"
cd /home/tacitus/hadoop-0.21.0/hdfs
ant compile -Doffline=1 jar
echo > /home/tacitus/hadoop-0.21.0/bin/../logs/hadoop-tacitus-namenode-se2.log
echo "========================================================================================================="
echo "copy build file to their target"
cp /home/tacitus/hadoop-0.21.0/hdfs/build/hadoop-hdfs-0.21.1-SNAPSHOT.jar  /home/tacitus/hadoop-0.21.0/
cp /home/tacitus/hadoop-0.21.0/hdfs/build/hadoop-hdfs-0.21.1-SNAPSHOT-sources.jar  /home/tacitus/hadoop-0.21.0/
scp /home/tacitus/hadoop-0.21.0/hadoop-hdfs-0.21.1-SNAPSHOT.jar  tacitus@netmonitor:/home/tacitus/hadoop-0.21.0/
scp /home/tacitus/hadoop-0.21.0/hadoop-hdfs-0.21.1-SNAPSHOT-sources.jar  tacitus@netmonitor:/home/tacitus/hadoop-0.21.0/
/home/tacitus/hadoop-0.21.0/bin/start-dfs.sh
cd -

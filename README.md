# HadoopProject

few Useful comments.
ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
chmod 0600 ~/.ssh/authorized_keys

ssh localhost
hdfs namenode -format
PDSH_RCMD_TYPE=ssh start-all.sh


hdfs dfs -mkdir /user
hdfs dfs -mkdir /user/Alekhya 
hdfs dfs -mkdir /user/Alekhya/input

hdfs dfs -put big.txt /user/saquib/input


hdfs dfs -rm -r /outputCountWords
hadoop jar Classdemo.jar WordCount /user/Alekhya/input/big.txt /outputCountWords

hdfs dfs -ls /outputCountWords
hdfs dfs -get /outputCountWords/part-r-00000 /home/Alekhya/eclipse-workspace


** This is for sample homework problem **
hdfs dfs -put data.txt /user/Alekhya/input
hdfs dfs -put mutual.txt /user/Alekhya/input
hdfs dfs -rm -r /outputFriends
hadoop jar Question_2.jar Question_2 /user/Alekhya/input/mutual.txt /outputFriends
hdfs dfs -get /outputFriends/part-r-00000 /home/Alekhya/eclipse-workspace

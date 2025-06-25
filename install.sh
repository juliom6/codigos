#!/bin/bash
sudo apt update && sudo apt install default-jdk -y
echo "export JAVA_HOME=$(dirname $(dirname $(update-alternatives --list javac)))" >> /home/luser/.bashrc
pip install gdown
gdown https://drive.google.com/uc?id=1G_sft8DS-Vb5xkeF6dnHFXzrIeNMC6hv -O spark-3.5.3-bin-hadoop3.tgz
tar xvf spark-3.5.3-bin-hadoop3.tgz
sudo mv spark-3.5.3-bin-hadoop3 /opt/spark
echo "export SPARK_HOME=/opt/spark" >> /home/luser/.bashrc
source /home/luser/.bashrc
echo "export PATH=$PATH:$SPARK_HOME/bin" >> /home/luser/.bashrc
source /home/luser/.bashrc
rm spark-3.5.3-bin-hadoop3.tgz
wget https://repo1.maven.org/maven2/com/microsoft/azure/azure-storage/8.6.6/azure-storage-8.6.6.jar
mv azure-storage-8.6.6.jar /opt/spark/jars/
wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-azure/3.3.1/hadoop-azure-3.3.1.jar
mv hadoop-azure-3.3.1.jar /opt/spark/jars/
wget https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.2.0/delta-spark_2.12-3.2.0.jar
mv delta-spark_2.12-3.2.0.jar /opt/spark/jars/
wget https://repo1.maven.org/maven2/io/delta/delta-storage/2.3.0/delta-storage-2.3.0.jar
mv delta-storage-2.3.0.jar /opt/spark/jars/

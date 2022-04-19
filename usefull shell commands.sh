# assign the desired permission to the folder of the project for accessing code + data
sudo chmod 700 -R the/location/of/your/project/med_network_analysis/
# start the ssh client and server
sudo service ssh --full-restart
# start hadoop
start-dfs.sh
# copy the data to hadoop file system
hdfs dfs -put med_network_analysis/data/ hdfs://localhost:9000/user/
# start pyspark including a package for reading xml files into spark dataframes and a package for processing graphs built
# upon spark dataframes
pyspark --packages com.databricks:spark-xml_2.12:0.12.0,graphframes:graphframes:0.8.1-spark3.0-s_2.12
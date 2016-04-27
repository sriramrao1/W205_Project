cd /data
git clone https://github.com/sriramrao1/project1.git 
wget http://s3.amazonaws.com/sriram-w205-project/yelp_dataset_challenge_academic_dataset.tar
tar -zxvf yelp_dataset_challenge_academic_dataset.tar

hdfs dfs -mkdir /user/w205/project1

hdfs dfs -mkdir /user/w205/project1/yelpbusinessdata

hdfs dfs -mkdir /user/w205/project1/yelpreviewdata

hdfs dfs -put /data/project1/yelp_academic_dataset_business.json /user/w205/project1/yelpbusinessdata

hdfs dfs -put /data/project1/yelp_academic_dataset_review.json /user/w205/project1/yelpreviewdata

http://sriram-w205-project.s3.amazonaws.com/yelp_academic_dataset_business.json
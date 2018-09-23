## Set Up Guid

* [Set up spark in mac via homebrew](https://medium.freecodecamp.org/installing-scala-and-apache-spark-on-mac-os-837ae57d283f)
* [Set up jupyter notebook for pyspark](https://medium.com/@GalarnykMichael/install-spark-on-mac-pyspark-453f395f240b)
* [Set up spark in aws](https://medium.com/@josemarcialportilla/getting-spark-python-and-jupyter-notebook-running-on-amazon-ec2-dec599e1c297)


### Access to s3 remotely

1. Create AccessID and AccessKey using [IAM](https://www.youtube.com/watch?v=UqKWHZ36yEM) in AWS
2. Export AccessID and AccessKey to environmental path
3. Run spark-shell or pyspark in console to start spark instance [additional reference](https://stackoverflow.com/questions/50183915/how-can-i-read-from-s3-in-pyspark-running-in-local-mode)
```
# check your jars file to determine hadoop-aws:[version]
# location of jars file: /usr/local/Cellar/apache-spark/2.3.1/libexec/jars
pyspark --packages org.apache.hadoop:hadoop-aws:2.7.3 # if you want to use multiple cpus --master local[2]
```
4. connect to s3
```
myAccessKey = os.getenv('AWS_ACCESS_KEY_ID')
mySecretKey = os.getenv('AWS_SECRET_ACCESS_KEY')

sc._jsc.hadoopConfiguration().set('fs.s3a.access.key', myAccessKey)
sc._jsc.hadoopConfiguration().set('fs.s3a.secret.key', mySecretKey)

textFile = sc.textFile("s3a://{}/{}".format(bucket, key))

textFile.count()
```

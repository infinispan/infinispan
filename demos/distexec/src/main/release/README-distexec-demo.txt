Infinispan distributed executors and MapReduce demo
---------------------------------------------------

Infinispan distributed executors and MapReduce demos can be run either locally (using by default the distributed-udp.xml configuration) or on Amazon EC2
(using the distributed-ec2.xml configuration).


Setup
-----

First of all extract your Infinispan distribution file. In bin directory of distribution you will find all demo scripts and among them runAppxDemo.sh and 
runWordCountDemo.sh


Background
----------

Infinispan distributed executors demo is essentially described by InfinispanPiAppxDemo class. Each time demo script runAppxDemo.sh is run it creates 
either a master or a slave Infinispan node (slave by default). In case of slave node Infinispan node joins the cluster and waits for a master node. 
Master node runs the PI approximation across cluster of Infinispan slave nodes. To see all options for a demo invoke runAppxDemo.sh with "--help" 
parameter.

Infinispan MapReduce demo is described by WordCountDemo class. Each time demo script runWordCountDemo.sh is run it creates 
either a master or a slave Infinispan node (slave by default). In case of slave node Infinispan node joins the cluster and waits for a master node. 
Booting slave nodes should be used as an opportunity to load up a text file into Infinispan grid. You can use using --textFile option to specify a text 
file to load. For example, you can use works of Shakespeare from http://www.gutenberg.org/ in txt format. Master node runs  word count map reduce 
across cluster of Infinispan slave nodes and finds k-th most frequent word across all data on the grid. To see all options for a demo invoke 
runWordCountDemo.sh with "--help" parameter.


EC2 Configuration
-----------------

Out of the box this demo uses the jgroups S3_ping functionality to create the cluster. By default it is intended to run on Amazon EC2 cloud. 
This feature uses an storage bucket on the Amazon S3 service to store the cluster details. This configuration is contained in 
etc/jgroups-s3_ping-aws.xml. This file requires the users aws key and secret aws key to be filled out before it can connect to S3 successfully. 
To do this just modify the following line in the file with the correct values: 
<S3_PING secret_access_key="my secret access key" access_key="my access key" location="my s3 bucket name" />

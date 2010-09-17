Infinispan EC2 demo
-------------------

The purpose of this demo is to 
* Demonstrate that Infinispan can run using Amazon EC2 instances as cluster nodes
* Demonstrate the Infinispan distributed cache mode where data is loaded into the cache from multiple different nodes
* Demonstrate the S3_Ping functionality of JGroups which allows nodes in the cluster to find each other

The demo itself consists of 
* A number of script files which run different java processes which either load or query the distributed caches.
* A very basic wep application which can join the cluster and retrieve data from different nodes in the cluster

The caches themselves are populated with data extracted from the Influenza gene database which is one of the freely available data sets hosted by Amazon 
(see http://developer.amazonwebservices.com/connect/entry.jspa?externalID=2419&categoryID=279)
This data is stored in etc/Amazon-TestData in gzipped format. Unzip before using.

There are three caches created
------------------------------
InfluenzaCache -> populated with data read from the Influenza.dat file, approx 82k entries
ProteinCache   -> populated with data read from the Influenza_aa.dat file, approx 102k entries
NucleotideCache-> populated with data read from the Influenza_na.dat file, approx 82k entries

The data relationship is as follows, each influenza cache entry has a related nucleotide and each nucleotide has one or more proteins.
To query the cache there is a query script which prompts the user to enter a influenza GAN (first column in influenza.dat) and if found the related data is extracted
from the other caches and displayed. 

Setup
--------------
Out of the box this demo uses the jgroups S3_ping functionality to create the cluster. This feature uses an storage bucket on the Amazon S3 service to store the cluster details.
This configuration is contained in etc/jgroups-s3_ping-aws.xml. This file requires the users aws key and secret aws key to be filled out before it can connect to S3 successfully.
To do this just modify the following line in the file with the correct values<S3_PING secret_access_key="my secret access key" access_key="my access key" location="my s3 bucket name" />

The location of the jgroups configuration file is specified using the EC2Demo-jgroups-config JVM system property. 
It jgroups S3_ping configuration can be replaced with a gossip router if required, a sample jgroups configuraton file is in the etc directory, jgroups-gossiprouter-aws.xml

Scripts
-------
runEC2Demo-all  		- creates the caches and loads the contents of the data files into the cache
runEC2Demo-influenza 		- creates the influenza cache and load the data from the influenza.dat file
runEC2Demo-protein		- creates the protein cache and load the data from the influenza_aa.dat file
runEC2Demo-nucleotide		- creates the nucleotide cacahe and load the data from the influenza_na.dat file
runEC2Demo-query		- attaches to the clustered caches, prompts the user for the influenza virus id and then searches the caches for the relevant data
runEC2Demo-reader		- attaches to the clustered caches and search for random data contained in the cache

Web Application
---------------
The web ui is war file which is deployed into a basic JBoss container.
A number of supporting libraries are required in order for the app to work.
The easiest to get this going is to use the jboss default profile and add the following libraries to the default/lib directory

jgroups-2.9.0.GA.jar
marshalling-api-1.2.0.GA.jar
river-1.2.0.GA.jar
infinispan-all.jar
infinispan-ec2-demo.jar

Deploy the infinispan-ec2-demo-web.war to the default/deploy directory

The location of the jgroups configuration file is also required for the web app to work.
There are two methods by which to do this. Either specify the file using the EC2Demo-jgroups-config e.g. -DEC2Demo-jgroups-config=/home/test/etc/jgroups-s3_ping-aws.xml
This property can also be specified in the default/deploy/properties-service.xml
or extract and modify the WEB-INF/web/xml changing the jgroups_file servlet context parameter to point to the file location. If both are specified then the system property takes priority.

Sample usage
------------
Using EC2 start up 4 EC2 small instance nodes.
Once up and running, on node 1 start up a JBoss instance on one of the nodes and deploy the infinispan-ec2-demo-web.war
On node 2, startup runEC2Demo-influenza.sh
On node 3, startup runEC2Demo-protein.sh
On node 4, startup runEC2Demo-nucleotide.sh
Point a web browser to the jboss node and enter the following "http://hostname:port/infinispan-ec2-demo-web/"
A web page will appear prompting for a Genbank Accession Number, enter "AB000607" and press return. The GBANs are the first column in the influenza.dat file e.g. AB000604,AB000605,AB000606,AB000607 etc
The virus details will then be displayed along with the addresses of the nodes in the cluster.

Possible issues
* Nodes don't see each other. Make sure that the firewall isn't blocking the inter-node communications
* Ensure that the correct jgroups properties are being used.
* If using S3_Ping you can check the cluster member details by using a utility such as s3sync see s3sync.net
* Make sure you are using the right version of jgroups: the one shipped with Infinispan. 2.6 or lower doesn't have s3_ping.



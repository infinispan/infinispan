Infinispan EC2 demo
-------------------

The purpose of this demo is to illustrate how to setup and use a non-trivial distributed cache using Amazon Web Services for the underlying infrastructure.
The demo itself consists of a number of script files which run different java processes which either load or query the distributed caches.
The caches themselves are populated with data extracted from the Influenza gene database which is one of the freely available data sets hosted by Amazon 
(see http://developer.amazonwebservices.com/connect/entry.jspa?externalID=2419&categoryID=279)
This data is stored in etc/Amazon-TestData in gzipped format. Unzip before using.

There are three caches created
InfluenzaCache -> populated with data read from the Influenza.dat file, approx 82k entries
ProteinCache   -> populated with data read from the Influenza_aa.dat file, approx 102k entries
NucleotideCache-> populated with data read from the Influenza_na.dat file, approx 82k entries

The data relationship is as follows, each influenza cache entry has a related nucleotide and each nucleotide has one or more proteins.
To query the cache there is a query script which prompts the user to enter a influenza GAN (first column in influenza.dat) and if found the related data is extracted
from the other caches and displayed. 
A future enhancement will provide a web UI to perform the search and display the results.

Setup
--------------
Out of the box this demo uses the jgroups S3_ping functionality to create the cluster. This feature uses an storage bucket on the Amazon S3 service to store the cluster details.
This configuration is contained in etc/jgroups-s3_ping-aws.xml. This file requires the users aws key and secret aws key to be filled out before it can connect to S3 successfully.
To do this just modify the following line in the file with the correct values<S3_PING secret_access_key="my secret access key" access_key="my access key" location="my s3 bucket name" />

The location of the jgroups configuration file is specified using the EC2Demo-jgroups-config system property. 
It jgroups S3_ping configuration can be replaced with a gossip router if required, a sample jgroups configuraton file is in the etc directory, jgroups-gossiprouter-aws.xml

Scripts
-------
runEC2Demo-all  		- creates the caches and loads the contents of the data files into the cache
runEC2Demo-influenza 	- creates the influenza cache and load the data from the influenza.dat file
runEC2Demo-protein		- creates the protein cache and load the data from the influenza_aa.dat file
runEC2Demo-nucleotide	- creates the nucleotide cacahe and load the data from the influenza_na.dat file
runEC2Demo-query		- attaches to the clustered caches, prompts the user for the influenza virus id and then searches the caches for the relevant data
runEC2Demo-reader		- attaches to the clustered caches and displays the number of elements in the cache




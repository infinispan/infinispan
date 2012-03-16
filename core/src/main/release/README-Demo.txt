Infinispan GUI Demo
-------------------

Infinispan comes with a GUI demo, to visually demonstrate state being moved around a cluster.

The demo is packaged in the infinispan-<version>-all.ZIP and infinispan-<version>-bin.ZIP distributions.

The demo can be launched using the bin/runGuiDemo.sh script from the project root.  Note that the configuration file
used by the demo can be changed by modifying runGuiDemo.sh to pass in

   -Dinfinispan.demo.cfg=file:/path/to/config.xml

to your JVM.  This allows you to try out and visualize different configurations.  If you are running the demo in a
clustered mode, it would make sense to launch several instances of the demo and watch data move around across different
instances.

Alternatively, you can pass the config file directly to runGuiDemo.sh, e.g. runGuiDemo.sh config-samples/relay1.xml.


Running the Infinispan GUI demo over 2 data centers
---------------------------------------------------

If you have 2 local clusters (data centers), you can use JGroups' RELAY to bridge the 2 and make them appear as if they
were a (virtual) global cluster. Consult the JGroups manual on www.jgroups.org for details.

There are 5 configuration files in config-samples which are needed to do this:

* relay1.xml and relay2.xml: these configure an Infinispan instance. The relay1.xml configuration points to
  config-samples/jgroups-relay1.xml, which is the JGroups configuration including RELAY. Same for relay2.xml.

* jgroups-relay1.xml and jgroups-relay2.xml define configurations for 2 different local clusters (data centers): they
  both include RELAY (configured with jgroups-tcp.xml to be used as bridge), and only differ in mcast_addr and
  mcast_port, and in the site ("nyc" or "zrh").

* jgroups-tcp.xml: this configured the JGroups bridge to be used between 2 data centers


To run:
* Start a few instances in the NYC cluster: ./runGuiDemo.sh config-samples/relay1.xml (note that config-samples has to be
  on the classpath)

* Start a few instances in the ZRH cluster: ./runGuiDemo.sh config-samples/relay2.xml.

* You should see that all of the ZRH and NYC nodes are listed a members of the same virtual cluster
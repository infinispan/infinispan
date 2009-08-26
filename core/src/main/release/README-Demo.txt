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


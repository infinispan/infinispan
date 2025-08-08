package org.infinispan.server.test.core;

import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.jboss.shrinkwrap.api.Archive;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class InfinispanServerTestConfiguration {

   //site names must match the same names defined in xsite-stacks.xml
   public static final String LON = "LON";
   public static final String NYC = "NYC";

   //each site needs a different discovery port otherwise they will be merged
   private static final Map<String, Integer> SITE_DISCOVER_PORTS_OFFSET;
   private static final int DEFAULT_DISCOVER_PORT = 46655;

   static {
      //46655 is the default! don't use it!
      SITE_DISCOVER_PORTS_OFFSET = Map.of(LON, 1000, NYC, 2000);
   }

   private final String configurationFile;
   private final int numServers;
   private final int expectedServers;
   private final ServerRunMode runMode;
   private final Properties properties;
   private final String[] mavenArtifacts;
   private final Archive<?>[] archives;
   private final boolean jmx;
   private final boolean parallelStartup;
   private final List<InfinispanServerListener> listeners;
   private final boolean defaultFile;
   private final String clusterName;
   private final String site;
   private final int portOffset;
   private final String[] features;
   private final String[] dataFiles;

   public InfinispanServerTestConfiguration(String configurationFile, int numServers, int expectedServers,
                                            ServerRunMode runMode, Properties properties, String[] mavenArtifacts,
                                            Archive<?>[] archives, boolean jmx, boolean parallelStartup,
                                            boolean defaultFile, List<InfinispanServerListener> listeners, String clusterName, String site,
                                            int portOffset, String[] features, String[] dataFiles) {
      this.configurationFile = configurationFile;
      this.numServers = numServers;
      this.expectedServers = expectedServers;
      this.runMode = runMode;
      this.properties = properties;
      this.mavenArtifacts = mavenArtifacts;
      this.archives = archives;
      this.jmx = jmx;
      this.parallelStartup = parallelStartup;
      this.defaultFile = defaultFile;
      this.listeners = List.copyOf(listeners);
      this.clusterName = clusterName;
      this.site = site;
      this.portOffset = portOffset;
      this.features = features;
      this.dataFiles = dataFiles;
   }

   public String configurationFile() {
      return configurationFile;
   }

   public int numServers() {
      return numServers;
   }

   public int expectedServers() {
      return expectedServers;
   }

   public ServerRunMode runMode() {
      return runMode;
   }

   public Properties properties() {
      return properties;
   }

   public Archive<?>[] archives() {
      return archives;
   }

   public boolean isJMXEnabled() {
      return jmx;
   }

   public String[] mavenArtifacts() {
      return mavenArtifacts;
   }

   public boolean isParallelStartup() {
      return parallelStartup;
   }

   public boolean isDefaultFile() {
      return defaultFile;
   }

   public List<InfinispanServerListener> listeners() {
      return listeners;
   }

   public String getClusterName() {
      return clusterName;
   }

   public String site() {
      return site;
   }

   public int siteDiscoveryPort() {
      return DEFAULT_DISCOVER_PORT + sitePortOffset();
   }

   public int sitePortOffset() {
      return SITE_DISCOVER_PORTS_OFFSET.get(site);
   }

   public int getPortOffset() {
      return portOffset;
   }

   public String[] getFeatures() {
      return features;
   }

   public String[] getDataFiles() {
      return dataFiles;
   }
}

package org.infinispan.server.test.core;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.jboss.shrinkwrap.api.spec.JavaArchive;

/**
 * Common code for JUnit 4 and Junit 5 Extension
 *
 * @param <T> type of builder
 * @author Katia Aresti
 * @since 11
 */
public abstract class AbstractServerConfigBuilder<T extends AbstractServerConfigBuilder<T>> {
   private final String configurationFile;
   private final boolean defaultFile;
   private final Properties properties;
   private String[] mavenArtifacts;
   private int numServers = 2;
   private int expectedServers = -1;
   private ServerRunMode runMode = ServerRunMode.DEFAULT;
   private JavaArchive[] archives;
   private String[] features;
   private boolean jmx;
   private boolean parallelStartup = true;
   private final List<InfinispanServerListener> listeners = new ArrayList<>();
   private String clusterName;
   private String siteName;
   private int portOffset = 0;
   private String[] dataFiles;

   protected AbstractServerConfigBuilder(String configurationFile, boolean defaultFile) {
      this.configurationFile = configurationFile;
      this.defaultFile = defaultFile;

      this.properties = new Properties();
      Properties sysProps = System.getProperties();
      for (String prop : sysProps.stringPropertyNames()) {
         if (prop.startsWith("org.infinispan")) {
            properties.put(prop,  sysProps.getProperty(prop));
         }
      }
   }

   public InfinispanServerTestConfiguration createServerTestConfiguration() {
      return new InfinispanServerTestConfiguration(configurationFile, numServers, expectedServers, runMode, this.properties, mavenArtifacts,
                  archives, jmx, parallelStartup, defaultFile, listeners, clusterName, siteName, portOffset, features, dataFiles);
   }

   public T mavenArtifacts(String... mavenArtifacts) {
      this.mavenArtifacts = mavenArtifacts;
      return (T) this;
   }

   public T numServers(int numServers) {
      this.numServers = numServers;
      return (T) this;
   }

   public T expectedServers(int expectedServers) {
      if (expectedServers >= 0 && expectedServers < numServers) {
         throw new IllegalStateException("Expected " + expectedServers + " number of servers must be greater than or equal to " + numServers + "number of servers being added");
      }
      this.expectedServers = expectedServers;
      return (T) this;
   }

   public T runMode(ServerRunMode serverRunMode) {
      this.runMode = serverRunMode;
      return (T) this;
   }

   public T featuresEnabled(String... features) {
      this.features = features;
      return (T) this;
   }

   public T addListener(InfinispanServerListener listener) {
      listeners.add(listener);
      return (T) this;
   }

   /**
    * Deployments
    */
   public T artifacts(JavaArchive... archives) {
      this.archives = archives;
      return (T) this;
   }

   /**
    * Define a system property
    */
   public T property(String name, String value) {
      this.properties.setProperty(name, value);
      return (T) this;
   }

   /**
    * Removes a property that was either previously defined or imported from the current running system properties
    * @param name the name of the property to remove
    * @return this to allow chain invocation
    */
   public T removeProperty(String name) {
      this.properties.remove(name);
      return (T) this;
   }

   public T enableJMX() {
      this.jmx = true;
      return (T) this;
   }

   /**
    * If false servers are started individually, waiting until they become available, before subsequent servers are started.
    */
   public T parallelStartup(boolean parallel) {
      this.parallelStartup = parallel;
      return (T) this;
   }

   /**
    * Sets the cluster name - if not specified the name of the driver will be used instead
    */
   public T clusterName(String clusterName) {
      this.clusterName = clusterName;
      return (T) this;
   }

   /**
    * Sets the current site name
    */
   public T site(String site) {
      this.siteName = site;
      return (T) this;
   }

   public String site() {
      return this.siteName;
   }

   public T portOffset(int port) {
      this.portOffset = port;
      return (T) this;
   }

   public T dataFiles(String... dataFiles) {
      this.dataFiles = dataFiles;
      return (T) this;
   }
}

package org.infinispan.server.test;

import java.util.Properties;

import org.jboss.shrinkwrap.api.spec.JavaArchive;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class InfinispanServerTestConfiguration {

   private final String configurationFile;
   private final int numServers;
   private final ServerRunMode runMode;
   private final Properties properties;
   private final String[] mavenArtifacts;
   private final JavaArchive[] archives;
   private final boolean jmx;

   public InfinispanServerTestConfiguration(String configurationFile, int numServers, ServerRunMode runMode, Properties properties, String[] mavenArtifacts, JavaArchive[] archives, boolean jmx) {
      this.configurationFile = configurationFile;
      this.numServers = numServers;
      this.runMode = runMode;
      this.properties = properties;
      this.mavenArtifacts = mavenArtifacts;
      this.archives = archives;
      this.jmx = jmx;
   }

   public String configurationFile() {
      return configurationFile;
   }

   public int numServers() {
      return numServers;
   }

   public ServerRunMode runMode() {
      return runMode;
   }

   public Properties properties() {
      return properties;
   }

   public JavaArchive[] archives() {
      return archives;
   }

   public boolean isJMXEnabled() {
      return jmx;
   }

   public String[] mavenArtifacts() {
      return mavenArtifacts;
   }
}

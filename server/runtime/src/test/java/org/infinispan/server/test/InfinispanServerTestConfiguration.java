package org.infinispan.server.test;

import java.util.Properties;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class InfinispanServerTestConfiguration {

   private final String configurationFile;
   private int numServers = 2;
   private ServerRunMode runMode = ServerRunMode.DEFAULT;
   private Properties properties = new Properties();
   private String[] artifacts;

   public InfinispanServerTestConfiguration(String configurationFile) {
      this.configurationFile = configurationFile;
   }

   /**
    * The number of servers in the initial cluster
    * @param numServers
    */
   public InfinispanServerTestConfiguration numServers(int numServers) {
      this.numServers = numServers;
      return this;
   }

   /**
    * The {@link ServerRunMode} to use. The default run mode is EMBEDDED unless overridden via the org.infinispan.test.server.driver system property
    * @param runMode
    */
   public InfinispanServerTestConfiguration runMode(ServerRunMode runMode) {
      this.runMode = runMode;
      return this;
   }

   /**
    * A system property
    */
   public InfinispanServerTestConfiguration property(String name, String value) {
      this.properties.setProperty(name, value);
      return this;
   }

   /**
    * Extra libs
    * @param artifacts
    */
   public InfinispanServerTestConfiguration artifacts(String... artifacts) {
      this.artifacts = artifacts;
      return this;
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

   public String[] artifacts() {
      return artifacts;
   }
}

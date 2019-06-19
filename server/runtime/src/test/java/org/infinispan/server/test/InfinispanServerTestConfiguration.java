package org.infinispan.server.test;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class InfinispanServerTestConfiguration {

   private final String configurationFile;
   private int numServers = 2;
   private ServerRunMode runMode = ServerRunMode.EMBEDDED;

   public InfinispanServerTestConfiguration(String configurationFile) {
      this.configurationFile = configurationFile;
   }

   public InfinispanServerTestConfiguration numServers(int numServers) {
      this.numServers = numServers;
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
}

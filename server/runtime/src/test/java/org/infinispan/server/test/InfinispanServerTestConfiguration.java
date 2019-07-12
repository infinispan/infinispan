package org.infinispan.server.test;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class InfinispanServerTestConfiguration {

   private final String configurationFile;
   private int numServers = 2;
   private ServerRunMode runMode = ServerRunMode.DEFAULT;

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
    * The {@link ServerRunMode} to use. The default run mode is EMBEDDED unless overridden via system property
    * @param runMode
    */
   public InfinispanServerTestConfiguration runMode(ServerRunMode runMode) {
      this.runMode = runMode;
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

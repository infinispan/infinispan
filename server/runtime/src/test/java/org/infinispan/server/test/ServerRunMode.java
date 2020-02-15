package org.infinispan.server.test;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public enum ServerRunMode {
   EMBEDDED {
      @Override
      public InfinispanServerDriver newDriver(InfinispanServerTestConfiguration configuration) {
         return new EmbeddedInfinispanServerDriver(configuration);
      }
   },
   CONTAINER {
      @Override
      public InfinispanServerDriver newDriver(InfinispanServerTestConfiguration configuration) {
         return new ContainerInfinispanServerDriver(configuration);
      }
   },
   DEFAULT {
      @Override
      public InfinispanServerDriver newDriver(InfinispanServerTestConfiguration configuration) {
         String driverName = System.getProperty(TestSystemPropertyNames.INFINISPAN_TEST_SERVER_DRIVER, EMBEDDED.name());
         ServerRunMode driver = ServerRunMode.valueOf(driverName);
         return driver.newDriver(configuration);
      }
   };

   public abstract InfinispanServerDriver newDriver(InfinispanServerTestConfiguration configuration);
}

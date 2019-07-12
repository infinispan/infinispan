package org.infinispan.server.test;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public enum ServerRunMode {
   EMBEDDED {
      @Override
      InfinispanServerDriver newDriver(InfinispanServerTestConfiguration configuration) {
         return new EmbeddedInfinispanServerDriver(configuration);
      }
   },
   CONTAINER {
      @Override
      InfinispanServerDriver newDriver(InfinispanServerTestConfiguration configuration) {
         return new ContainerInfinispanServerDriver(configuration);
      }
   },
   DEFAULT {
      @Override
      InfinispanServerDriver newDriver(InfinispanServerTestConfiguration configuration) {
         String driverName = System.getProperty("org.infinispan.test.server.driver", EMBEDDED.name());
         ServerRunMode driver = ServerRunMode.valueOf(driverName);
         return driver.newDriver(configuration);
      }
   };

   abstract InfinispanServerDriver newDriver(InfinispanServerTestConfiguration configuration);
}

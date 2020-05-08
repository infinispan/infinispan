package org.infinispan.server.test.core;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public enum ServerRunMode {
   EMBEDDED {
      @Override
      public AbstractInfinispanServerDriver newDriver(InfinispanServerTestConfiguration configuration) {
         return new EmbeddedInfinispanServerDriver(configuration);
      }
   },
   CONTAINER {
      @Override
      public AbstractInfinispanServerDriver newDriver(InfinispanServerTestConfiguration configuration) {
         return new ContainerInfinispanServerDriver(configuration);
      }
   },
   FORKED {
      @Override
      public AbstractInfinispanServerDriver newDriver(InfinispanServerTestConfiguration configuration) {
         return new ForkedInfinispanServerDriver(configuration);
      }
   },
   DEFAULT {
      @Override
      public AbstractInfinispanServerDriver newDriver(InfinispanServerTestConfiguration configuration) {
         ServerRunMode driver;
         if (configuration.properties().containsKey(TestSystemPropertyNames.INFINISPAN_TEST_SERVER_BASE_IMAGE_NAME)) {
            driver = CONTAINER;
         } else {
            String driverName = configuration.properties().getProperty(TestSystemPropertyNames.INFINISPAN_TEST_SERVER_DRIVER, EMBEDDED.name());
            driver = ServerRunMode.valueOf(driverName);
         }
         return driver.newDriver(configuration);
      }
   };

   public abstract AbstractInfinispanServerDriver newDriver(InfinispanServerTestConfiguration configuration);
}

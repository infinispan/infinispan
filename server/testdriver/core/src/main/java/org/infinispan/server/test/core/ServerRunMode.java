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
   // When running the test in the server side, it is only allowed to do remote call to a remote container
   REMOTE_CONTAINER {
      @Override
      public AbstractInfinispanServerDriver newDriver(InfinispanServerTestConfiguration configuration) {
         return new RemoteContainerInfinispanServerDriver(configuration);
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

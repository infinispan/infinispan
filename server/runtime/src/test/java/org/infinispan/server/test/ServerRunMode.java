package org.infinispan.server.test;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public enum ServerRunMode {
   EMBEDDED,
   CONTAINER;

   public InfinispanServerDriver newDriver(InfinispanServerTestConfiguration configuration) {
      switch (this) {
         case EMBEDDED:
            return new EmbeddedInfinispanServerDriver(configuration);
         case CONTAINER:
            return new ContainerInfinispanServerDriver(configuration);
      }
      return null; // Unreachable
   }
}

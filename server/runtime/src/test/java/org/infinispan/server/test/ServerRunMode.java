package org.infinispan.server.test;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public enum ServerRunMode {
   EMBEDDED,
   FORKED,
   CONTAINER;

   public ServerDriver newDriver(String name, ServerTestConfiguration configuration) {
      switch (this) {
         case EMBEDDED:
            return new EmbeddedServerDriver(name, configuration);
         case FORKED:
            return new ForkedServerDriver(name, configuration);
         case CONTAINER:
            return new ContainerServerDriver(name, configuration);
      }
      return null; // Unreachable
   }
}

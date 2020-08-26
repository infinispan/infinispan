package org.infinispan.server.test.junit5;

import static org.infinispan.server.test.core.AbstractInfinispanServerDriver.DEFAULT_CLUSTERED_INFINISPAN_CONFIG_FILE_NAME;

import org.infinispan.server.test.core.AbstractServerConfigBuilder;
import org.infinispan.server.test.core.ServerRunMode;

/**
 * Infinispan Server Extension Builder
 *
 * @author Katia Aresti
 * @since 11
 */
public class InfinispanServerExtensionBuilder extends AbstractServerConfigBuilder<InfinispanServerExtensionBuilder> {
   /**
    * Use this method to instantiate a single clustered server in a container, using the default configuration.
    */
   public static InfinispanServerExtension server() {
      return new InfinispanServerExtensionBuilder(DEFAULT_CLUSTERED_INFINISPAN_CONFIG_FILE_NAME, true)
            .numServers(1)
            .runMode(ServerRunMode.CONTAINER)
            .parallelStartup(false)
            .build();
   }

   /**
    * Use this method to instantiate a single clustered server in a container, using a custom configuration.
    */
   public static InfinispanServerExtension server(String configurationFile) {
      return new InfinispanServerExtensionBuilder(configurationFile, false)
            .numServers(1)
            .runMode(ServerRunMode.CONTAINER)
            .parallelStartup(false)
            .build();
   }

   public static InfinispanServerExtensionBuilder config(String configurationFile) {
      return new InfinispanServerExtensionBuilder(configurationFile, false);
   }

   private InfinispanServerExtensionBuilder(String configurationFile, boolean defaultFile) {
      super(configurationFile, defaultFile);
   }

   public InfinispanServerExtension build() {
      return new InfinispanServerExtension(createServerTestConfiguration());
   }
}

package org.infinispan.server.test.junit5;

import org.infinispan.server.test.core.AbstractServerConfigBuilder;
import org.infinispan.server.test.core.ServerRunMode;

import static org.infinispan.server.test.core.AbstractInfinispanServerDriver.DEFAULT_CLUSTERED_INFINISPAN_CONFIG_FILE_NAME;

/**
 * Infinispan Server Extension Builder
 *
 * @author Katia Aresti
 * @since 11
 */
public class InfinispanServerExtensionBuilder extends AbstractServerConfigBuilder<InfinispanServerExtensionBuilder> {
   /**
    * Use this method to instantiate a single clustered embedded server
    *
    * @return InfinispanServerExtension
    */
   public static InfinispanServerExtension server() {
      return new InfinispanServerExtensionBuilder(DEFAULT_CLUSTERED_INFINISPAN_CONFIG_FILE_NAME, true)
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

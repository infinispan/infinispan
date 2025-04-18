package org.infinispan.server.test.junit5;

import static org.infinispan.server.test.core.AbstractInfinispanServerDriver.DEFAULT_CLUSTERED_INFINISPAN_CONFIG_FILE_NAME;

import org.infinispan.server.test.core.AbstractServerConfigBuilder;
import org.infinispan.server.test.core.ServerRunMode;

/**
 * Builds and configures {@link InfinispanServerExtension} instances for testing.
 * <p>
 * Provides convenient methods for starting Infinispan server containers with
 * default or custom configurations.
 * </p>
 *
 * <p>Examples:</p>
 <ul>
 *   <li>
 *     Run with version-matching image:
 *     <pre>{@code
 *       @RegisterExtension
 *       static InfinispanServerExtension infinispanServerExtension = InfinispanServerExtensionBuilder.server();
 *     }</pre>
 *   </li>
 *   <li>
 *     Run with a custom dev image, using the default server configuration file:
 *     <pre>{@code
 *     @RegisterExtension
 *     static InfinispanServerExtension infinispanServerExtension = InfinispanServerExtensionBuilder
 *         .config()
 *         .numServers(1)
 *         .runMode(ServerRunMode.CONTAINER)
 *         .property(TestSystemPropertyNames.INFINISPAN_TEST_SERVER_BASE_IMAGE_NAME,
 *                   "quay.io/infinispan/server:15.0")
 *         .build();
 *     }</pre>
 *   </li>
 * </ul>
 *
 *
 * @author Katia Aresti
 * @since 11
 */
public class InfinispanServerExtensionBuilder extends AbstractServerConfigBuilder<InfinispanServerExtensionBuilder> {
   /**
    * Creates a single clustered server using the default configuration in a container.
    *
    * @return an {@link InfinispanServerExtension} instance
    */
   public static InfinispanServerExtension server() {
      return new InfinispanServerExtensionBuilder(DEFAULT_CLUSTERED_INFINISPAN_CONFIG_FILE_NAME, true)
            .numServers(1)
            .runMode(ServerRunMode.CONTAINER)
            .parallelStartup(false)
            .build();
   }

   /**
    * Creates a single clustered server using a custom configuration in a container.
    *
    * @param configurationFile custom config file path
    * @return an {@link InfinispanServerExtension} instance
    */
   public static InfinispanServerExtension server(String configurationFile) {
      return new InfinispanServerExtensionBuilder(configurationFile, false)
            .numServers(1)
            .runMode(ServerRunMode.CONTAINER)
            .parallelStartup(false)
            .build();
   }

   /**
    * Starts a builder with the default configuration.
    * Using the {@link org.infinispan.server.test.core.AbstractInfinispanServerDriver#DEFAULT_CLUSTERED_INFINISPAN_CONFIG_FILE_NAME}
    *
    * @return a new {@link InfinispanServerExtensionBuilder}
    */
   public static InfinispanServerExtensionBuilder config() {
      return new InfinispanServerExtensionBuilder(DEFAULT_CLUSTERED_INFINISPAN_CONFIG_FILE_NAME, true);
   }

   /**
    * Starts a builder with a custom configuration and flag for default handling.
    *
    * @param configurationFile config file path or a default config file name
    * @param defaultFile whether it's the default config present in the server distribution
    * @return a new {@link InfinispanServerExtensionBuilder}
    */
   public static InfinispanServerExtensionBuilder config(String configurationFile, boolean defaultFile) {
      return new InfinispanServerExtensionBuilder(configurationFile, defaultFile);
   }

   /**
    * Starts a builder with a custom configuration.
    *
    * @param configurationFile config file path
    * @return a new {@link InfinispanServerExtensionBuilder}
    */
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

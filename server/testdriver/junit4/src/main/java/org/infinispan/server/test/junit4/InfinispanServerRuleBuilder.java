package org.infinispan.server.test.junit4;

import static org.infinispan.server.test.core.AbstractInfinispanServerDriver.DEFAULT_CLUSTERED_INFINISPAN_CONFIG_FILE_NAME;

import org.infinispan.server.test.core.AbstractServerConfigBuilder;
import org.infinispan.server.test.core.ServerRunMode;

/**
 * Builder for {@link InfinispanServerRule}.
 *
 * @author Diego Lovison &lt;dlovison@redhat.com&gt;
 * @author Dan Berindei
 * @since 10.0
 **/
public class InfinispanServerRuleBuilder extends AbstractServerConfigBuilder<InfinispanServerRuleBuilder> {
   /**
    * Use this method to instantiate a single clustered embedded server
    *
    * @return InfinispanServerRule
    */
   public static InfinispanServerRule server() {
      return server(true);
   }

   public static InfinispanServerRule server(boolean container) {
      return new InfinispanServerRuleBuilder(DEFAULT_CLUSTERED_INFINISPAN_CONFIG_FILE_NAME, true)
            .numServers(1)
            .runMode(container? ServerRunMode.CONTAINER : ServerRunMode.EMBEDDED)
            .parallelStartup(false)
            .build();
   }

   public static InfinispanServerRule server(String configurationFile) {
      return new InfinispanServerRuleBuilder(configurationFile, false)
            .numServers(1)
            .runMode(ServerRunMode.CONTAINER)
            .parallelStartup(false)
            .build();
   }

   public static InfinispanServerRuleBuilder config(String configurationFile) {
      return new InfinispanServerRuleBuilder(configurationFile, false);
   }

   private InfinispanServerRuleBuilder(String configurationFile, boolean defaultFile) {
      super(configurationFile, defaultFile);
   }

   public InfinispanServerRule build() {
      return new InfinispanServerRule(createServerTestConfiguration());
   }
}

package org.infinispan;

import org.infinispan.configuration.cache.Configuration;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.factories.annotations.InfinispanModule;
import org.infinispan.lifecycle.ModuleLifecycle;
import org.infinispan.remoting.transport.Transport;

/**
 * @private
 */
@InfinispanModule(name = "core")
public class CoreModule implements ModuleLifecycle {

   @Override
   public void creatingConfiguration(GlobalComponentRegistry grc, Configuration configuration) {
      if (configuration.sites().hasEnabledBackups()) {
         grc.getComponent(Transport.class).checkCrossSiteAvailable();
      }
   }
}

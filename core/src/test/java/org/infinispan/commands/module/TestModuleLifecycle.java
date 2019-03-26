package org.infinispan.commands.module;

import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.lifecycle.ModuleLifecycle;
import org.kohsuke.MetaInfServices;

/**
 * @author Dan Berindei
 * @since 9.4
 */
@MetaInfServices(ModuleLifecycle.class)
public final class TestModuleLifecycle implements ModuleLifecycle {

   @Override
   public void cacheManagerStarting(GlobalComponentRegistry gcr, GlobalConfiguration globalConfiguration) {
      TestGlobalConfiguration testGlobalConfiguration = globalConfiguration.module(TestGlobalConfiguration.class);
      if (testGlobalConfiguration != null) {
         testGlobalConfiguration.getComponents().forEach((componentName, instance) -> {
            gcr.registerComponent(instance, componentName);
         });
      }
   }
}

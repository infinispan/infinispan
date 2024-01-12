package ${package};

import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.factories.annotations.InfinispanModule;
import org.infinispan.lifecycle.ModuleLifecycle;

@InfinispanModule(name = "custom-module", requiredModules = "core")
public class CustomModule implements ModuleLifecycle {
   @Override
   public void cacheManagerStarting(GlobalComponentRegistry gcr, GlobalConfiguration globalConfiguration) {
      CustomModuleConfiguration config = globalConfiguration.module(CustomModuleConfiguration.class);
      // The config object will be null if the custom module hasn't been configured declaratively via JSON/XML/Yaml
      // or programmatically when creating the EmbeddedCacheManager GlobalConfiguration
      if (config != null) {
         // Print out welcome message from CustomModuleConfiguration. If the message attribute hasn't been overridden in
         // the config, then the default message is printed
         System.out.println("Custom Module Message: " + config.message());
      }
   }
}

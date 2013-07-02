package org.infinispan.loaders.jpa;

import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.lifecycle.AbstractModuleLifecycle;

public class JpaCacheStoreLifecycleManager extends AbstractModuleLifecycle {
   @Override
   public void cacheManagerStarting(GlobalComponentRegistry gcr, GlobalConfiguration globalConfiguration) {
      gcr.registerComponent(new EntityManagerFactoryRegistry(), EntityManagerFactoryRegistry.class);
   }
   
   @Override
   public void cacheManagerStopping(GlobalComponentRegistry gcr) {
      gcr.getComponent(EntityManagerFactoryRegistry.class).closeAll();
   }
}

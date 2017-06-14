package org.infinispan.persistence.jpa.impl;

import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.lifecycle.ModuleLifecycle;
import org.kohsuke.MetaInfServices;

@MetaInfServices(org.infinispan.lifecycle.ModuleLifecycle.class)
public class JpaStoreLifecycleManager implements ModuleLifecycle {
   @Override
   public void cacheManagerStarting(GlobalComponentRegistry gcr, GlobalConfiguration globalConfiguration) {
      gcr.registerComponent(new EntityManagerFactoryRegistry(), EntityManagerFactoryRegistry.class);
   }

   @Override
   public void cacheManagerStopping(GlobalComponentRegistry gcr) {
      gcr.getComponent(EntityManagerFactoryRegistry.class).closeAll();
   }
}

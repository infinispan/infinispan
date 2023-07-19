package org.infinispan.server.insights.mock;

import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.factories.annotations.InfinispanModule;
import org.infinispan.lifecycle.ModuleLifecycle;
import org.infinispan.server.core.DummyServerManagement;
import org.infinispan.server.core.ServerManagement;

@InfinispanModule(name = "server-runtime", requiredModules = {"core", "server-core"})
public class RuntimeMockModule implements ModuleLifecycle {

   @Override
   public void cacheManagerStarted(GlobalComponentRegistry gcr) {
      gcr.registerComponent(new DummyServerManagement(gcr.getCacheManager()), ServerManagement.class);
   }
}

package org.infinispan.tasks.impl;

import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.factories.annotations.InfinispanModule;
import org.infinispan.lifecycle.ModuleLifecycle;
import org.infinispan.tasks.TaskManager;

/**
 * LifecycleCallbacks.
 *
 * @author Tristan Tarrant
 * @since 8.1
 */
@InfinispanModule(name = "tasks", requiredModules = "core")
public class LifecycleCallbacks implements ModuleLifecycle {

   @Override
   public void cacheManagerStarting(GlobalComponentRegistry gcr, GlobalConfiguration gc) {
      if (gcr.getComponent(TaskManager.class) == null)
         gcr.registerComponent(new TaskManagerImpl(), TaskManager.class);
   }
}

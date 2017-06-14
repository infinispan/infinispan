package org.infinispan.tasks.impl;

import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.lifecycle.ModuleLifecycle;
import org.infinispan.tasks.TaskManager;
import org.kohsuke.MetaInfServices;

/**
 * LifecycleCallbacks.
 *
 * @author Tristan Tarrant
 * @since 8.1
 */
@MetaInfServices(ModuleLifecycle.class)
public class LifecycleCallbacks implements ModuleLifecycle {

   @Override
   public void cacheManagerStarting(GlobalComponentRegistry gcr, GlobalConfiguration gc) {
      if (gcr.getComponent(TaskManager.class) == null)
         gcr.registerComponent(new TaskManagerImpl(), TaskManager.class);
   }
}

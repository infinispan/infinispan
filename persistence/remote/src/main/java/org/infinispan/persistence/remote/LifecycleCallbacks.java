package org.infinispan.persistence.remote;

import java.util.Map;

import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.lifecycle.ModuleLifecycle;
import org.infinispan.persistence.remote.upgrade.MigrationTask;
import org.infinispan.persistence.remote.upgrade.RemovedFilter;

/**
 * @author gustavonalle
 * @since 8.2
 */
public class LifecycleCallbacks implements ModuleLifecycle {

   @Override
   public void cacheManagerStarting(GlobalComponentRegistry gcr, GlobalConfiguration globalCfg) {
      Map<Integer, AdvancedExternalizer<?>> externalizerMap = globalCfg.serialization().advancedExternalizers();
      externalizerMap.put(ExternalizerIds.MIGRATION_TASK, new MigrationTask.Externalizer());
      externalizerMap.put(ExternalizerIds.REMOVED_FILTER, new RemovedFilter.Externalizer());
   }

}

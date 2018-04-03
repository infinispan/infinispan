package org.infinispan.scripting.impl;

import java.util.Map;

import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.factories.annotations.InfinispanModule;
import org.infinispan.lifecycle.ModuleLifecycle;
import org.infinispan.scripting.ScriptingManager;

/**
 * LifecycleCallbacks.
 *
 * @author Tristan Tarrant
 * @since 7.2
 */
@InfinispanModule(name = "scripting", requiredModules = "core")
public class LifecycleCallbacks implements ModuleLifecycle {

   @Override
   public void cacheManagerStarting(GlobalComponentRegistry gcr, GlobalConfiguration gc) {
      ScriptingManagerImpl scriptingManager = new ScriptingManagerImpl();
      gcr.registerComponent(scriptingManager, ScriptingManager.class);

      Map<Integer,AdvancedExternalizer<?>> externalizerMap = gc.serialization().advancedExternalizers();
      externalizerMap.put(ExternalizerIds.SCRIPT_METADATA, new ScriptMetadata.Externalizer());
   }
}

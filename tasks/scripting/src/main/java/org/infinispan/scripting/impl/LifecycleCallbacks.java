package org.infinispan.scripting.impl;

import java.util.Map;

import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.lifecycle.AbstractModuleLifecycle;
import org.infinispan.lifecycle.ModuleLifecycle;
import org.infinispan.scripting.ScriptingManager;
import org.kohsuke.MetaInfServices;

/**
 * LifecycleCallbacks.
 *
 * @author Tristan Tarrant
 * @since 7.2
 */
@MetaInfServices(ModuleLifecycle.class)
public class LifecycleCallbacks extends AbstractModuleLifecycle {

   @Override
   public void cacheManagerStarting(GlobalComponentRegistry gcr, GlobalConfiguration gc) {
      ScriptingManagerImpl scriptingManager = new ScriptingManagerImpl();
      gcr.registerComponent(scriptingManager, ScriptingManager.class);

      Map<Integer,AdvancedExternalizer<?>> externalizerMap = gc.serialization().advancedExternalizers();
      externalizerMap.put(ExternalizerIds.SCRIPT_METADATA, new ScriptMetadata.Externalizer());
   }
}

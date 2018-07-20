package org.infinispan.scripting.impl;

import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.factories.annotations.InfinispanModule;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.factories.impl.BasicComponentRegistry;
import org.infinispan.lifecycle.ModuleLifecycle;
import org.infinispan.marshall.persistence.PersistenceMarshaller;
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

      BasicComponentRegistry bcr = gcr.getComponent(BasicComponentRegistry.class);
      PersistenceMarshaller persistenceMarshaller = bcr.getComponent(KnownComponentNames.PERSISTENCE_MARSHALLER, PersistenceMarshaller.class).wired();
      persistenceMarshaller.register(new PersistenceContextInitializerImpl());
   }
}

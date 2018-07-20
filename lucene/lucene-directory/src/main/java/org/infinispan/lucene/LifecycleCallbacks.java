package org.infinispan.lucene;

import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.factories.annotations.InfinispanModule;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.factories.impl.BasicComponentRegistry;
import org.infinispan.lifecycle.ModuleLifecycle;
import org.infinispan.lucene.impl.PersistenceContextInitializerImpl;
import org.infinispan.marshall.persistence.PersistenceMarshaller;

/**
 * Module lifecycle callbacks implementation that enables module specific
 * {@link org.infinispan.commons.marshall.AdvancedExternalizer} implementations to be registered.
 *
 * @author Galder Zamarreño
 * @author Sanne Grinovero
 * @since 5.0
 */
@InfinispanModule(name = "lucene-directory", requiredModules = "core")
public class LifecycleCallbacks implements ModuleLifecycle {

   @Override
   public void cacheManagerStarting(GlobalComponentRegistry gcr, GlobalConfiguration globalCfg) {
      registerSerializationContext(gcr);
   }

   public static void registerSerializationContext(GlobalComponentRegistry gcr) {
      BasicComponentRegistry bcr = gcr.getComponent(BasicComponentRegistry.class);
      PersistenceMarshaller persistenceMarshaller = bcr.getComponent(KnownComponentNames.PERSISTENCE_MARSHALLER, PersistenceMarshaller.class).wired();
      persistenceMarshaller.register(new PersistenceContextInitializerImpl());
   }
}

package org.infinispan.server.eventlogger.impl;

import java.util.Map;

import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.lifecycle.AbstractModuleLifecycle;
import org.infinispan.lifecycle.ModuleLifecycle;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.eventlogger.ServerEventLogManager;
import org.infinispan.util.logging.events.EventLogManager;
import org.kohsuke.MetaInfServices;

/**
 * LifecycleCallback for the server event logger module. Registers advanced externalizers and
 * initializes the server logger
 *
 * @author Tristan Tarrant
 * @since 8.2
 */
@MetaInfServices(ModuleLifecycle.class)
public class LifecycleCallbacks extends AbstractModuleLifecycle {

   @Override
   public void cacheManagerStarting(GlobalComponentRegistry gcr, GlobalConfiguration gc) {
      ServerEventLogManager eventLogManager = new ServerEventLogManagerImpl();
      gcr.registerComponent(eventLogManager, ServerEventLogManager.class);

      Map<Integer, AdvancedExternalizer<?>> externalizerMap = gc.serialization().advancedExternalizers();
      externalizerMap.put(ExternalizerIds.SERVER_EVENT, new ServerEventImpl.Externalizer());
   }

   @Override
   public void cacheManagerStarted(GlobalComponentRegistry gcr) {
      EmbeddedCacheManager cacheManager = gcr.getComponent(EmbeddedCacheManager.class);
      EventLogManager.replaceEventLogger(new ServerEventLogger(cacheManager, gcr.getTimeService()));
   }

   @Override
   public void cacheManagerStopping(GlobalComponentRegistry gcr) {
      //FIXME implement me
   }
}

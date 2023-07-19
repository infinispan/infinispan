package org.infinispan.server.insights;

import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.factories.annotations.InfinispanModule;
import org.infinispan.lifecycle.ModuleLifecycle;
import org.infinispan.server.core.ServerManagement;
import org.infinispan.server.insights.config.InsightsActivation;
import org.infinispan.server.insights.logging.Log;
import org.infinispan.util.concurrent.BlockingManager;
import org.jboss.logging.Logger;

@InfinispanModule(name = "insights", requiredModules = {"core", "server-runtime"})
public class InsightsModule implements ModuleLifecycle {

   public static final String REDHAT_INSIGHTS_ACTIVATION_PROPERTY_NAME = "infinispan.insights.activation";
   public static final String REPORT_VERSION = "1.0.0";

   public static final Log log = Logger.getMessageLogger(Log.class, "org.infinispan.SERVER");

   private InsightsService service;

   @Override
   public void cacheManagerStarted(GlobalComponentRegistry gcr) {
      InsightsActivation activation = activation();
      if (InsightsActivation.DISABLED.equals(activation)) {
         log.insightsDisabled();
         return;
      }
      ServerManagement server = gcr.getComponent(ServerManagement.class);
      if (server == null) {
         log.serverManagementLookupFailed();
         return;
      }

      service = new InsightsService(server);
      gcr.registerComponent(service, InsightsService.class);
      if (InsightsActivation.LOCAL.equals(activation)) {
         log.insightsLocallyEnabled();
         return;
      }

      log.insightsEnabled();
      BlockingManager blockingManager = gcr.getComponent(BlockingManager.class);
      service.start(blockingManager);
   }

   @Override
   public void cacheManagerStopping(GlobalComponentRegistry gcr) {
      service.stop();
   }

   private static InsightsActivation activation() {
      String activation = System.getProperty(REDHAT_INSIGHTS_ACTIVATION_PROPERTY_NAME);

      if (activation == null || activation.equalsIgnoreCase(InsightsActivation.LOCAL.name())) {
         return InsightsActivation.LOCAL;
      }
      if (activation.equalsIgnoreCase(InsightsActivation.DISABLED.name())) {
         return InsightsActivation.DISABLED;
      }
      if (activation.equalsIgnoreCase(InsightsActivation.ENABLED.name())) {
         return InsightsActivation.ENABLED;
      }

      log.insightsActivationNotValidValue(REDHAT_INSIGHTS_ACTIVATION_PROPERTY_NAME, activation);
      return InsightsActivation.LOCAL;
   }
}

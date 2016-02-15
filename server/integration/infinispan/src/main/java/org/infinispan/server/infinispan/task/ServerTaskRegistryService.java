package org.infinispan.server.infinispan.task;

import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.jboss.as.clustering.infinispan.InfinispanLogger;
import org.jboss.msc.service.*;

/**
 * Author: Michal Szynkiewicz, michal.l.szynkiewicz@gmail.com
 * Date: 1/20/16
 * Time: 12:53 PM
 */
@Scope(Scopes.GLOBAL)
public class ServerTaskRegistryService implements Service<ServerTaskRegistry> {
   public static final ServiceName SERVICE_NAME = ServiceName.JBOSS.append("DeployedTaskRegistry");
   private static final ServerTaskRegistry registry = new ServerTaskRegistryImpl();

   @Override
   public ServerTaskRegistry getValue() throws IllegalStateException, IllegalArgumentException {
      return registry;
   }

   @Override
   public void start(StartContext context) throws StartException {
      InfinispanLogger.ROOT_LOGGER.debugf("Starting DeployedTaskRegistryService");
   }

   @Override
   public void stop(StopContext context) {
      InfinispanLogger.ROOT_LOGGER.debugf("Stopping DeployedTaskRegistryService");
   }

}

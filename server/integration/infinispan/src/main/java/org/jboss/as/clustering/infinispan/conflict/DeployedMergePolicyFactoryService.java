package org.jboss.as.clustering.infinispan.conflict;

import org.infinispan.server.infinispan.spi.InfinispanSubsystem;
import org.jboss.as.clustering.infinispan.InfinispanLogger;
import org.jboss.msc.service.Service;
import org.jboss.msc.service.ServiceName;
import org.jboss.msc.service.StartContext;
import org.jboss.msc.service.StartException;
import org.jboss.msc.service.StopContext;

public class DeployedMergePolicyFactoryService implements Service<DeployedMergePolicyFactory> {

   public static final ServiceName SERVICE_NAME = InfinispanSubsystem.SERVICE_NAME_BASE.append("DeployedMergePolicyFactory");

   private final DeployedMergePolicyFactory factory = new DeployedMergePolicyFactory();

   @Override
   public void start(StartContext context) throws StartException {
      InfinispanLogger.ROOT_LOGGER.debugf("Starting DeployedMergePolicyFactoryService");
   }

   @Override
   public void stop(StopContext context) {
      InfinispanLogger.ROOT_LOGGER.debugf("Stopping DeployedMergePolicyFactoryService");
   }

   @Override
   public DeployedMergePolicyFactory getValue() throws IllegalStateException, IllegalArgumentException {
      return factory;
   }
}

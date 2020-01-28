package org.infinispan.server.endpoint.subsystem;

import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.server.core.AbstractProtocolServer;
import org.infinispan.server.core.configuration.ProtocolServerConfiguration;
import org.infinispan.server.endpoint.EndpointLogger;
import org.jboss.as.controller.OperationContext;
import org.jboss.as.controller.OperationFailedException;
import org.jboss.as.controller.OperationStepHandler;
import org.jboss.dmr.ModelNode;
import org.jboss.msc.service.ServiceController;
import org.jboss.msc.service.ServiceName;

public class ProtocolServerConnectorTransportOpHandler implements OperationStepHandler {
   private final String prefix;
   private final boolean stop;

   public ProtocolServerConnectorTransportOpHandler(String prefix, boolean stop) {
      this.prefix = prefix;
      this.stop = stop;
   }

   @Override
   public void execute(OperationContext context, ModelNode operation) throws OperationFailedException {
      ServiceName serviceName = EndpointUtils.getServiceName(operation, prefix);
      final ServiceController<?> controller = context.getServiceRegistry(false).getService(serviceName);
      AbstractProtocolServer<ProtocolServerConfiguration> protocolServer = (AbstractProtocolServer<ProtocolServerConfiguration>) controller.getValue();
      ComponentStatus previousStatus = protocolServer.getTransportStatus();
      if (stop) {
         protocolServer.stopTransport();
         if (!protocolServer.getTransportStatus().equals(previousStatus)) {
            EndpointLogger.ROOT_LOGGER.transportStopped(serviceName.getSimpleName());
         }
      } else {
         protocolServer.startTransport();
         if (!protocolServer.getTransportStatus().equals(previousStatus)) {
            EndpointLogger.ROOT_LOGGER.transportStarted(serviceName.getSimpleName());
         }
      }
   }
}

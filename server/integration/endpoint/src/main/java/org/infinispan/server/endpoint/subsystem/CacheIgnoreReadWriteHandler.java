package org.infinispan.server.endpoint.subsystem;

import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;

import org.infinispan.server.commons.msc.ServiceContainerHelper;
import org.infinispan.server.core.CacheIgnoreAware;
import org.infinispan.server.core.ProtocolServer;
import org.infinispan.server.endpoint.Constants;
import org.jboss.as.controller.AbstractWriteAttributeHandler;
import org.jboss.as.controller.AttributeDefinition;
import org.jboss.as.controller.OperationContext;
import org.jboss.as.controller.OperationFailedException;
import org.jboss.as.controller.PathAddress;
import org.jboss.as.controller.PathElement;
import org.jboss.as.controller.descriptions.ModelDescriptionConstants;
import org.jboss.dmr.ModelNode;
import org.jboss.msc.service.ServiceController;
import org.jboss.msc.service.ServiceName;

/**
 * @author gustavonalle
 * @since 8.1
 */
public class CacheIgnoreReadWriteHandler extends AbstractWriteAttributeHandler<Void> {

   public CacheIgnoreReadWriteHandler(final AttributeDefinition attribute) {
      super(attribute);
   }

   @Override
   protected boolean applyUpdateToRuntime(OperationContext context, ModelNode operation, String attributeName, ModelNode resolvedValue, ModelNode currentValue, HandbackHolder<Void> handbackHolder) throws OperationFailedException {
      CacheIgnoreAware cacheIgnoreAware = getCacheDisablingAware(operation, context);
      applyModelToRuntime(cacheIgnoreAware, resolvedValue);
      return false;
   }

   private CacheIgnoreAware getCacheDisablingAware(ModelNode operation, OperationContext context) {
      final PathAddress address = PathAddress.pathAddress(operation.get(ModelDescriptionConstants.OP_ADDR));
      PathElement lastElement = address.getLastElement();
      String connectorType = lastElement.getKey().split("-")[0];
      String connectorName = lastElement.getValue();
      ServiceName serviceName = Constants.DATAGRID.append(connectorType, connectorName);
      ServiceController<ProtocolServer> service = ServiceContainerHelper.findService(context.getServiceRegistry(true), serviceName);
      return service.getValue();
   }

   @Override
   protected void revertUpdateToRuntime(OperationContext context, ModelNode operation, String attributeName, ModelNode valueToRestore, ModelNode valueToRevert, Void handback) throws OperationFailedException {
      CacheIgnoreAware cacheIgnoreAware = getCacheDisablingAware(operation, context);
      final ModelNode restored = context.readResource(PathAddress.EMPTY_ADDRESS).getModel().clone();
      restored.get(attributeName).set(valueToRestore);
      applyModelToRuntime(cacheIgnoreAware, restored);
   }

   private void applyModelToRuntime(CacheIgnoreAware cacheIgnoreAware, final ModelNode model) throws OperationFailedException {
      Set<String> values = Collections.emptySet();
      if (model.isDefined()) {
         values = model.asList().stream().map(ModelNode::asString).collect(Collectors.toSet());
      }
      cacheIgnoreAware.setIgnoredCaches(values);
   }

}

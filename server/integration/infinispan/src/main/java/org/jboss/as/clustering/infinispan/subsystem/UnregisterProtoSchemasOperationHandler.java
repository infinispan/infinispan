package org.jboss.as.clustering.infinispan.subsystem;

import static org.jboss.as.clustering.infinispan.InfinispanMessages.MESSAGES;
import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.OP_ADDR;

import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.remote.ProtobufMetadataManager;
import org.infinispan.server.infinispan.spi.service.CacheContainerServiceName;
import org.jboss.as.controller.OperationContext;
import org.jboss.as.controller.OperationFailedException;
import org.jboss.as.controller.OperationStepHandler;
import org.jboss.as.controller.PathAddress;
import org.jboss.dmr.ModelNode;
import org.jboss.dmr.ModelType;
import org.jboss.msc.service.ServiceController;

/**
 * Handler to unregister a bunch of protobuf schemas given their names.
 *
 * @author anistor@redhat.com
 * @since 8.2
 */
public class UnregisterProtoSchemasOperationHandler implements OperationStepHandler {

   public static final UnregisterProtoSchemasOperationHandler INSTANCE = new UnregisterProtoSchemasOperationHandler();

   @Override
   public void execute(OperationContext context, ModelNode operation) throws OperationFailedException {
      final PathAddress address = PathAddress.pathAddress(operation.require(OP_ADDR));
      final String cacheContainerName = address.getElement(address.size() - 1).getValue();
      final ServiceController<?> controller = context.getServiceRegistry(false).getService(
            CacheContainerServiceName.CACHE_CONTAINER.getServiceName(cacheContainerName));
      if (controller != null) {
         final EmbeddedCacheManager cacheManager = (EmbeddedCacheManager) controller.getValue();
         final ProtobufMetadataManager protoManager = cacheManager.getGlobalComponentRegistry().getComponent(ProtobufMetadataManager.class);
         if (protoManager != null) {
            try {
               ModelNode names = operation.require(CacheContainerResource.PROTO_NAMES.getName());
               validateParameters(names);
               for (ModelNode modelNode : names.asList()) {
                  protoManager.unregisterProtofile(modelNode.asString());
               }
            } catch (Exception e) {
               throw new OperationFailedException(MESSAGES.failedToInvokeOperation(e.getLocalizedMessage()));
            }
         }
      }
   }

   private void validateParameters(ModelNode names) {
      if (names.getType() != ModelType.LIST) {
         throw MESSAGES.invalidParameterType(CacheContainerResource.PROTO_NAMES.getName(), ModelType.LIST.toString());
      }
   }
}

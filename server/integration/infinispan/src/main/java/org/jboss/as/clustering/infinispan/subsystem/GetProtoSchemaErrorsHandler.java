package org.jboss.as.clustering.infinispan.subsystem;

import static org.jboss.as.clustering.infinispan.InfinispanMessages.MESSAGES;
import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.OP_ADDR;

import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.remote.ProtobufMetadataManager;
import org.infinispan.server.infinispan.spi.service.CacheContainerServiceName;
import org.jboss.as.controller.AbstractRuntimeOnlyHandler;
import org.jboss.as.controller.OperationContext;
import org.jboss.as.controller.OperationFailedException;
import org.jboss.as.controller.PathAddress;
import org.jboss.dmr.ModelNode;
import org.jboss.dmr.ModelType;
import org.jboss.msc.service.ServiceController;

/**
 * Handler to get the errors messages attached to a protobuf schema file (by name).
 *
 * @author anistor@redhat.com
 * @since 9.0
 */
public class GetProtoSchemaErrorsHandler extends AbstractRuntimeOnlyHandler {

   public static final GetProtoSchemaErrorsHandler INSTANCE = new GetProtoSchemaErrorsHandler();

   @Override
   public void executeRuntimeStep(OperationContext context, ModelNode operation) throws OperationFailedException {
      final PathAddress address = PathAddress.pathAddress(operation.require(OP_ADDR));
      final String cacheContainerName = address.getElement(address.size() - 1).getValue();
      final ServiceController<?> controller = context.getServiceRegistry(false).getService(
            CacheContainerServiceName.CACHE_CONTAINER.getServiceName(cacheContainerName));
      if (controller != null) {
         final EmbeddedCacheManager cacheManager = (EmbeddedCacheManager) controller.getValue();
         final ProtobufMetadataManager protoManager = cacheManager.getGlobalComponentRegistry().getComponent(ProtobufMetadataManager.class);

         if (protoManager != null) {
            try {
               ModelNode name = operation.require(CacheContainerResource.PROTO_NAME.getName());
               validateParameters(name);
               String fileErrors = protoManager.getFileErrors(name.asString());
               ModelNode result = new ModelNode();
               if (fileErrors != null) {
                  result.set(fileErrors);
               }
               context.getResult().set(result);
            } catch (Exception e) {
               throw new OperationFailedException(MESSAGES.failedToInvokeOperation(e.getLocalizedMessage()));
            }
         }
      }
   }

   private void validateParameters(ModelNode name) {
      if (name.getType() != ModelType.STRING) {
         throw MESSAGES.invalidParameterType(CacheContainerResource.PROTO_NAME.getName(), ModelType.STRING.toString());
      }
   }
}

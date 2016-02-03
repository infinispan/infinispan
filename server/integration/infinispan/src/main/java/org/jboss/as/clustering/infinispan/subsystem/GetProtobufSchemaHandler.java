package org.jboss.as.clustering.infinispan.subsystem;

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

import static org.jboss.as.clustering.infinispan.InfinispanMessages.MESSAGES;
import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.OP_ADDR;

/**
 * Handler to get the contents of a protobuf schema file by name.
 *
 * @author anistor@redhat.com
 * @since 8.2
 */
public class GetProtobufSchemaHandler extends AbstractRuntimeOnlyHandler {

   public static final GetProtobufSchemaHandler INSTANCE = new GetProtobufSchemaHandler();

   @Override
   public void executeRuntimeStep(OperationContext context, ModelNode operation) throws OperationFailedException {
      final PathAddress address = PathAddress.pathAddress(operation.require(OP_ADDR));
      final String cacheContainerName = address.getElement(address.size() - 1).getValue();
      final ServiceController<?> controller = context.getServiceRegistry(false).getService(
            CacheContainerServiceName.CACHE_CONTAINER.getServiceName(cacheContainerName));
      final EmbeddedCacheManager cacheManager = (EmbeddedCacheManager) controller.getValue();
      final ProtobufMetadataManager protoManager = cacheManager.getGlobalComponentRegistry().getComponent(ProtobufMetadataManager.class);

      if (protoManager != null) {
         try {
            ModelNode name = operation.require(CacheContainerResource.PROTO_NAME.getName());
            validateParameters(name);
            context.getResult().set(new ModelNode().set(protoManager.getProtofile(name.asString())));
         } catch (Exception e) {
            throw new OperationFailedException(MESSAGES.failedToInvokeOperation(e.getLocalizedMessage()));
         }
      }
   }

   private void validateParameters(ModelNode name) {
      if (name.getType() != ModelType.STRING) {
         throw MESSAGES.invalidParameterType(CacheContainerResource.PROTO_NAME.getName(), ModelType.STRING.toString());
      }
   }
}

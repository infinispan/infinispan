package org.jboss.as.clustering.infinispan.subsystem;

import static org.jboss.as.clustering.infinispan.InfinispanMessages.MESSAGES;
import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.OP_ADDR;

import java.util.ArrayList;
import java.util.List;

import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.remote.ProtobufMetadataManager;
import org.infinispan.server.infinispan.spi.service.CacheContainerServiceName;
import org.jboss.as.controller.AbstractRuntimeOnlyHandler;
import org.jboss.as.controller.OperationContext;
import org.jboss.as.controller.OperationFailedException;
import org.jboss.as.controller.PathAddress;
import org.jboss.dmr.ModelNode;
import org.jboss.msc.service.ServiceController;

/**
 * Handler to get the names of the registered protobuf schemas that have errors (syntactic or semantic).
 *
 * @author anistor@redhat.com
 * @since 9.0
 */
public class GetProtobufSchemasWithErrorsHandler extends AbstractRuntimeOnlyHandler {

   public static final GetProtobufSchemasWithErrorsHandler INSTANCE = new GetProtobufSchemasWithErrorsHandler();

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
            String[] fileNames = protoManager.getFilesWithErrors();
            ModelNode result = new ModelNode();
            if (fileNames != null) {
               List<ModelNode> models = new ArrayList<>(fileNames.length);
               for (String name : fileNames) {
                  models.add(new ModelNode().set(name));
               }
               result.set(models);
            } else {
               result.setEmptyList();
            }
            context.getResult().set(result);
         } catch (Exception e) {
            throw new OperationFailedException(MESSAGES.failedToInvokeOperation(e.getLocalizedMessage()));
         }
      }
   }
}

package org.jboss.as.clustering.infinispan.subsystem;

import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.remote.ProtobufMetadataManager;
import org.infinispan.server.infinispan.spi.service.CacheContainerServiceName;
import org.jboss.as.controller.AbstractRuntimeOnlyHandler;
import org.jboss.as.controller.OperationContext;
import org.jboss.as.controller.OperationFailedException;
import org.jboss.as.controller.PathAddress;
import org.jboss.dmr.ModelNode;
import org.jboss.msc.service.ServiceController;

import java.util.ArrayList;
import java.util.List;

import static org.jboss.as.clustering.infinispan.InfinispanMessages.MESSAGES;
import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.OP_ADDR;

/**
 * Handler to get the names of the registered protobuf schemas.
 *
 * @author anistor@redhat.com
 * @since 8.2
 */
public class GetProtobufSchemaNamesHandler extends AbstractRuntimeOnlyHandler {

   public static final GetProtobufSchemaNamesHandler INSTANCE = new GetProtobufSchemaNamesHandler();

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
            String[] names = protoManager.getProtofileNames();
            List<ModelNode> models = new ArrayList<ModelNode>(names.length);
            for (String name : names) {
               models.add(new ModelNode().set(name));
            }
            context.getResult().set(new ModelNode().set(models));
         } catch (Exception e) {
            throw new OperationFailedException(MESSAGES.failedToInvokeOperation(e.getLocalizedMessage()));
         }
      }
   }
}

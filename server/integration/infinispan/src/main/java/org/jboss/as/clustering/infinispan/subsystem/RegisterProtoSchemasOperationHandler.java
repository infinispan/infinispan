package org.jboss.as.clustering.infinispan.subsystem;

import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.remote.ProtobufMetadataManager;
import org.jboss.as.controller.OperationContext;
import org.jboss.as.controller.OperationFailedException;
import org.jboss.as.controller.OperationStepHandler;
import org.jboss.as.controller.PathAddress;
import org.jboss.dmr.ModelNode;
import org.jboss.dmr.ModelType;
import org.jboss.msc.service.ServiceController;

import java.util.List;

import static org.jboss.as.clustering.infinispan.InfinispanMessages.MESSAGES;
import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.OP_ADDR;

/**
 * Handler to register the proto file(s) contents directly, targeted to
 * RHQ plugin clients
 *
 * @author gustavonalle
 * @since 7.0
 */
public class RegisterProtoSchemasOperationHandler implements OperationStepHandler {

   public static final RegisterProtoSchemasOperationHandler INSTANCE = new RegisterProtoSchemasOperationHandler();

   @Override
   public void execute(OperationContext context, ModelNode operation) throws OperationFailedException {
      final PathAddress address = PathAddress.pathAddress(operation.require(OP_ADDR));
      final String cacheContainerName = address.getElement(address.size() - 1).getValue();
      final ServiceController<?> controller = context.getServiceRegistry(false).getService(
              EmbeddedCacheManagerService.getServiceName(cacheContainerName));

      EmbeddedCacheManager cacheManager = (EmbeddedCacheManager) controller.getValue();
      ProtobufMetadataManager protoManager = cacheManager.getGlobalComponentRegistry().getComponent(ProtobufMetadataManager.class);
      if (protoManager != null) {
         try {
            String namesParameter = CacheContainerResource.PROTO_NAMES.getName();
            String contentsParameter = CacheContainerResource.PROTO_CONTENTS.getName();
            ModelNode names = operation.require(namesParameter);
            ModelNode contents = operation.require(contentsParameter);
            validateParameters(names, contents);
            List<ModelNode> descriptorsNames = names.asList();
            List<ModelNode> descriptorsContents = contents.asList();
            String[] nameArray = new String[descriptorsNames.size()];
            String[] contentArray = new String[descriptorsNames.size()];
            int i = 0;
            for (ModelNode modelNode : descriptorsNames) {
               nameArray[i] = modelNode.asString();
               contentArray[i] = descriptorsContents.get(i).asString();
               i++;
            }
            protoManager.registerProtofiles(nameArray, contentArray);
         } catch (Exception e) {
            throw new OperationFailedException(new ModelNode().set(MESSAGES.failedToInvokeOperation(e.getLocalizedMessage())));
         }
      }
      context.stepCompleted();
   }

   private void validateParameters(ModelNode name, ModelNode contents) {
      String requiredType = ModelType.LIST.toString();
      String nameParameter = CacheContainerResource.PROTO_NAMES.getName();
      String contentParameter = CacheContainerResource.PROTO_CONTENTS.getName();
      if (name.getType() != ModelType.LIST) {
         throw MESSAGES.invalidParameterType(nameParameter, requiredType);
      }
      if (contents.getType() != ModelType.LIST) {
         throw MESSAGES.invalidParameterType(contentParameter, requiredType);
      }
      if (name.asList().size() != contents.asList().size()) {
         throw MESSAGES.invalidParameterSizes(nameParameter, contentParameter);
      }
   }

}

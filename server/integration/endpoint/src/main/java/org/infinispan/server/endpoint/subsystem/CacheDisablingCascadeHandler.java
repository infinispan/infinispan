package org.infinispan.server.endpoint.subsystem;

import java.util.function.BiFunction;
import java.util.stream.Stream;

import org.infinispan.server.commons.controller.Operations;
import org.jboss.as.controller.OperationContext;
import org.jboss.as.controller.OperationFailedException;
import org.jboss.as.controller.OperationStepHandler;
import org.jboss.as.controller.PathAddress;
import org.jboss.as.controller.PathElement;
import org.jboss.as.controller.descriptions.ModelDescriptionConstants;
import org.jboss.as.controller.registry.Resource;
import org.jboss.dmr.ModelNode;

/**
 * @author gustavonalle
 * @since 8.1
 */
public class CacheDisablingCascadeHandler implements OperationStepHandler {

   private final BiFunction<ModelNode, ModelNode, ModelNode> modelNodeOp;

   public CacheDisablingCascadeHandler(BiFunction<ModelNode, ModelNode, ModelNode> modelNodeOp) {
      this.modelNodeOp = modelNodeOp;
   }

   @Override
   public void execute(OperationContext context, ModelNode operation) throws OperationFailedException {
      final PathAddress address = PathAddress.pathAddress(operation.require(ModelDescriptionConstants.OP_ADDR));
      final ModelNode cacheNames = operation.get(ModelKeys.CACHE_NAMES);
      Resource endpointResource = context.readResourceForUpdate(PathAddress.EMPTY_ADDRESS);
      Stream<Resource.ResourceEntry> connectorResources = endpointResource.getChildTypes().stream().flatMap(type -> endpointResource.getChildren(type).stream());
      connectorResources.forEach(resourceEntry -> {
         ModelNode list = resourceEntry.getModel().get(ModelKeys.IGNORED_CACHES);
         ModelNode result = modelNodeOp.apply(list, cacheNames);
         PathElement pathElement = resourceEntry.getPathElement();
         ModelNode op = Operations.createWriteAttributeOperation(PathAddress.pathAddress(address, pathElement), ModelKeys.IGNORED_CACHES, result);
         context.addStep(op, new CacheIgnoreReadWriteHandler(CommonConnectorResource.IGNORED_CACHES), OperationContext.Stage.MODEL);
      });
   }

}

package org.infinispan.server.endpoint.subsystem;

import org.jboss.as.controller.OperationContext;
import org.jboss.as.controller.OperationFailedException;
import org.jboss.as.controller.OperationStepHandler;
import org.jboss.as.controller.PathAddress;
import org.jboss.as.controller.registry.Resource;
import org.jboss.as.controller.registry.Resource.ResourceEntry;
import org.jboss.dmr.ModelNode;

import java.util.List;
import java.util.stream.Collectors;

import static org.infinispan.server.endpoint.subsystem.ModelNodeUtils.contains;

/**
 * This handler inspects all endpoint resources to check if a one or more caches are being ignored.
 *
 * @author gustavonalle
 * @since 9.0
 */
public class CacheIgnoreStatusHandler implements OperationStepHandler {

   public static final CacheIgnoreStatusHandler INSTANCE = new CacheIgnoreStatusHandler();

   private CacheIgnoreStatusHandler() {
   }

   @Override
   public void execute(OperationContext context, ModelNode operation) throws OperationFailedException {
      final ModelNode ignoreSummary = new ModelNode();
      final ModelNode cacheNames = operation.get(ModelKeys.CACHE_NAMES);

      Resource endpointResource = context.readResource(PathAddress.EMPTY_ADDRESS);

      List<ResourceEntry> endpoints = endpointResource.getChildTypes().stream()
              .flatMap(type -> endpointResource.getChildren(type).stream()).collect(Collectors.toList());

      cacheNames.asList().stream().map(ModelNode::asString).forEach(cacheName -> {
         boolean ignoredAllEndpoints = endpoints.stream()
                 .map(endpoint -> endpoint.getModel().get(ModelKeys.IGNORED_CACHES))
                 .allMatch(ignoredList -> contains(ignoredList, cacheName));

         ignoreSummary.get(cacheName).set(ignoredAllEndpoints);
      });
      context.getResult().set(ignoreSummary);
   }

}

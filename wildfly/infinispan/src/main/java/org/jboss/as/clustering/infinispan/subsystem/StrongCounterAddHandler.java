package org.jboss.as.clustering.infinispan.subsystem;

import org.infinispan.counter.api.CounterConfiguration;
import org.jboss.as.controller.AttributeDefinition;
import org.jboss.as.controller.OperationContext;
import org.jboss.as.controller.OperationFailedException;
import org.jboss.dmr.ModelNode;

/**
 * Add operation handler for /subsystem=infinispan/cache-container=clustered/counter=*
 *
 * @author Vladimir Blagojevic
 *
 */
public class StrongCounterAddHandler extends CounterAddHandler {

   /**
    *
    */
   @Override
   void populate(ModelNode fromModel, ModelNode toModel) throws OperationFailedException {
      super.populate(fromModel, toModel);
      for (AttributeDefinition attr : StrongCounterResource.STRONG_ATTRIBUTES) {
         attr.validateAndSet(fromModel, toModel);
      }
   }

   /**
    * Implementation of abstract method processModelNode
    *
    */
   @Override
   void processModelNode(OperationContext context, ModelNode counter,
         CounterConfiguration.Builder builder) throws OperationFailedException {
      super.processModelNode(context, counter, builder);

      ModelNode upperBoundModel = counter.get(ModelKeys.UPPER_BOUND);
      ModelNode lowerBoundModel = counter.get(ModelKeys.LOWER_BOUND);
      if (lowerBoundModel.isDefined()) {
          Long lowerBound = StrongCounterResource.LOWER_BOUND.resolveModelAttribute(context, counter).asLong();
          builder.lowerBound(lowerBound);
      }
      if (upperBoundModel.isDefined()) {
          Long upperBound = StrongCounterResource.UPPER_BOUND.resolveModelAttribute(context, counter).asLong();
          builder.upperBound(upperBound);
      }
   }
}

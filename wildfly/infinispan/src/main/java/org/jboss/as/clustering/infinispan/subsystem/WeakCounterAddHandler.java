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
public class WeakCounterAddHandler extends CounterAddHandler {

   @Override
   void populate(ModelNode fromModel, ModelNode toModel) throws OperationFailedException {
      super.populate(fromModel, toModel);
      for (AttributeDefinition attr : WeakCounterResource.WEAK_ATTRIBUTES) {
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

      Integer concurrency = WeakCounterResource.CONCURRENCY_LEVEL.resolveModelAttribute(context, counter).asInt();
      builder.concurrencyLevel(concurrency);
   }
}

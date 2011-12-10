package org.infinispan.configuration.cache;

import static org.infinispan.transaction.TransactionMode.NON_TRANSACTIONAL;

import org.infinispan.transaction.TransactionMode;

public class InvocationBatchingConfigurationBuilder extends AbstractConfigurationChildBuilder<InvocationBatchingConfiguration> {

   private boolean enabled = false;
   
   InvocationBatchingConfigurationBuilder(ConfigurationBuilder builder) {
      super(builder);
   }
   
   public InvocationBatchingConfigurationBuilder enable() {
      this.enabled = true;
      return this;
   }
   
   public InvocationBatchingConfigurationBuilder disable() {
      this.enabled = false;
      return this;
   }

   @Override
   void validate() {
      if (enabled && getBuilder().transaction().transactionMode.equals(NON_TRANSACTIONAL))
         throw new IllegalStateException("Cannot enable Invocation Batching when the Transaction Mode is NON_TRANSACTIONAL, set the transaction mode to TRANSACTIONAL");
   }

   @Override
   InvocationBatchingConfiguration create() {
      return new InvocationBatchingConfiguration(enabled);
   }
   
}

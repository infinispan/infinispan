package org.infinispan.configuration.cache;

import static org.infinispan.transaction.TransactionMode.NON_TRANSACTIONAL;

import org.infinispan.commons.configuration.Builder;

public class InvocationBatchingConfigurationBuilder extends AbstractConfigurationChildBuilder implements Builder<InvocationBatchingConfiguration> {

   boolean enabled = false;

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

   public InvocationBatchingConfigurationBuilder enable(boolean enable) {
      this.enabled = enable;
      return this;
   }

   @Override
   public void validate() {
      if (enabled && getBuilder().transaction().transactionMode != null && getBuilder().transaction().transactionMode.equals(NON_TRANSACTIONAL))
         throw new IllegalStateException("Cannot enable Invocation Batching when the Transaction Mode is NON_TRANSACTIONAL, set the transaction mode to TRANSACTIONAL");
   }

   @Override
   public InvocationBatchingConfiguration create() {
      return new InvocationBatchingConfiguration(enabled);
   }

   @Override
   public InvocationBatchingConfigurationBuilder read(InvocationBatchingConfiguration template) {
      this.enabled = template.enabled();

      return this;
   }

   @Override
   public String toString() {
      return "InvocationBatchingConfigurationBuilder{" +
            "enabled=" + enabled +
            '}';
   }

}

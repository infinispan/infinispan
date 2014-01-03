package org.infinispan.configuration.cache;

import static org.infinispan.transaction.TransactionMode.NON_TRANSACTIONAL;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

public class InvocationBatchingConfigurationBuilder extends AbstractConfigurationChildBuilder implements Builder<InvocationBatchingConfiguration> {

   private static final Log log = LogFactory.getLog(InvocationBatchingConfigurationBuilder.class);

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
         throw log.invocationBatchingNeedsTransactionalCache();
      if (enabled && getBuilder().transaction().recovery().enabled && !getBuilder().transaction().useSynchronization())
         throw log.invocationBatchingCannotBeRecoverable();
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

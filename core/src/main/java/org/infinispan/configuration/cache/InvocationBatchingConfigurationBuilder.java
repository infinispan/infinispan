package org.infinispan.configuration.cache;

import static org.infinispan.configuration.cache.InvocationBatchingConfiguration.ENABLED;
import static org.infinispan.util.logging.Log.CONFIG;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.Combine;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.configuration.global.GlobalConfiguration;

@Deprecated(since = "16.3", forRemoval = true)
public class InvocationBatchingConfigurationBuilder extends AbstractConfigurationChildBuilder implements Builder<InvocationBatchingConfiguration> {

   private final AttributeSet attributes;

   InvocationBatchingConfigurationBuilder(ConfigurationBuilder builder) {
      super(builder);
      attributes = InvocationBatchingConfiguration.attributeDefinitionSet();
   }

   @Override
   public AttributeSet attributes() {
      return attributes;
   }

   public InvocationBatchingConfigurationBuilder enable() {
      attributes.attribute(ENABLED).set(true);
      enableTransactions();
      return this;
   }

   public InvocationBatchingConfigurationBuilder disable() {
      attributes.attribute(ENABLED).set(false);
      return this;
   }

   public InvocationBatchingConfigurationBuilder enable(boolean enable) {
      attributes.attribute(ENABLED).set(enable);
      if (enable) {
         enableTransactions();
      }
      return this;
   }

   private void enableTransactions() {
      TransactionConfigurationBuilder txBuilder = getBuilder().transaction();
      if (!txBuilder.attributes.attribute(TransactionConfiguration.MODE).isModified()
            && txBuilder.transactionModeOverride == null) {
         txBuilder.transactionModeOverride = org.infinispan.transaction.TransactionMode.TRANSACTIONAL;
      }
   }

   boolean isEnabled() {
      return attributes.attribute(ENABLED).get();
   }

   @Override
   public void validate() {
      if (isEnabled()) {
         org.infinispan.configuration.cache.TransactionMode mode = getBuilder().transaction().resolveMode();
         if (!mode.isTransactional())
            throw CONFIG.invocationBatchingNeedsTransactionalCache();
         if (mode.isRecoveryEnabled() || getBuilder().transaction().recovery().isEnabled())
            throw CONFIG.invocationBatchingCannotBeRecoverable();
      }
   }

   @Override
   public void validate(GlobalConfiguration globalConfig) {
   }

   @Override
   public InvocationBatchingConfiguration create() {
      return new InvocationBatchingConfiguration(attributes.protect());
   }

   @Override
   public InvocationBatchingConfigurationBuilder read(InvocationBatchingConfiguration template, Combine combine) {
      attributes.read(template.attributes(), combine);

      return this;
   }

   @Override
   public String toString() {
      return "InvocationBatchingConfigurationBuilder [attributes=" + attributes + "]";
   }
}

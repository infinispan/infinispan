package org.infinispan.configuration.cache;

import static org.infinispan.configuration.cache.InvocationBatchingConfiguration.ENABLED;
import static org.infinispan.configuration.cache.TransactionConfiguration.TRANSACTION_MODE;
import static org.infinispan.transaction.TransactionMode.TRANSACTIONAL;
import static org.infinispan.util.logging.Log.CONFIG;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.transaction.TransactionMode;

public class InvocationBatchingConfigurationBuilder extends AbstractConfigurationChildBuilder implements Builder<InvocationBatchingConfiguration> {

   private final AttributeSet attributes;

   InvocationBatchingConfigurationBuilder(ConfigurationBuilder builder) {
      super(builder);
      attributes = InvocationBatchingConfiguration.attributeDefinitionSet();
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
      Attribute<TransactionMode> transactionModeAttribute =
            getBuilder().transaction().attributes().attribute(TRANSACTION_MODE);
      if (!transactionModeAttribute.isModified()) {
         getBuilder().transaction().transactionMode(TRANSACTIONAL);
      }
   }

   boolean isEnabled() {
      return attributes.attribute(ENABLED).get();
   }

   @Override
   public void validate() {
      if (isEnabled() && !getBuilder().transaction().transactionMode().isTransactional())
         throw CONFIG.invocationBatchingNeedsTransactionalCache();
      if (isEnabled() && getBuilder().transaction().recovery().isEnabled() &&
          !getBuilder().transaction().useSynchronization())
         throw CONFIG.invocationBatchingCannotBeRecoverable();
   }

   @Override
   public void validate(GlobalConfiguration globalConfig) {
   }

   @Override
   public InvocationBatchingConfiguration create() {
      return new InvocationBatchingConfiguration(attributes.protect());
   }

   @Override
   public InvocationBatchingConfigurationBuilder read(InvocationBatchingConfiguration template) {
      attributes.read(template.attributes());

      return this;
   }

   @Override
   public String toString() {
      return "InvocationBatchingConfigurationBuilder [attributes=" + attributes + "]";
   }
}

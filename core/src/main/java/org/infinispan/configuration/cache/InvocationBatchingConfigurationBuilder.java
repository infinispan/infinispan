package org.infinispan.configuration.cache;

import static org.infinispan.configuration.cache.InvocationBatchingConfiguration.ENABLED;
import static org.infinispan.transaction.TransactionMode.NON_TRANSACTIONAL;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

public class InvocationBatchingConfigurationBuilder extends AbstractConfigurationChildBuilder implements Builder<InvocationBatchingConfiguration> {

   private static final Log log = LogFactory.getLog(InvocationBatchingConfigurationBuilder.class);

   private final AttributeSet attributes;

   InvocationBatchingConfigurationBuilder(ConfigurationBuilder builder) {
      super(builder);
      attributes = InvocationBatchingConfiguration.attributeDefinitionSet();
   }

   public InvocationBatchingConfigurationBuilder enable() {
      attributes.attribute(ENABLED).set(true);
      return this;
   }

   public InvocationBatchingConfigurationBuilder disable() {
      attributes.attribute(ENABLED).set(false);
      return this;
   }

   public InvocationBatchingConfigurationBuilder enable(boolean enable) {
      attributes.attribute(ENABLED).set(enable);
      return this;
   }

   boolean isEnabled() {
      return attributes.attribute(ENABLED).get();
   }

   @Override
   public void validate() {
      if (isEnabled()  && getBuilder().transaction().transactionMode() != null && getBuilder().transaction().transactionMode().equals(NON_TRANSACTIONAL))
         throw log.invocationBatchingNeedsTransactionalCache();
      if (isEnabled() && getBuilder().transaction().recovery().isEnabled() && !getBuilder().transaction().useSynchronization())
         throw log.invocationBatchingCannotBeRecoverable();
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

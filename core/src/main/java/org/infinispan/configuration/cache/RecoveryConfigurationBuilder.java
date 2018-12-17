package org.infinispan.configuration.cache;

import static org.infinispan.configuration.cache.RecoveryConfiguration.ENABLED;
import static org.infinispan.configuration.cache.RecoveryConfiguration.RECOVERY_INFO_CACHE_NAME;

import java.lang.invoke.MethodHandles;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.ConfigurationBuilderInfo;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.elements.ElementDefinition;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.transaction.TransactionProtocol;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Defines recovery configuration for the cache.
 *
 * @author pmuir
 *
 */
public class RecoveryConfigurationBuilder extends AbstractTransportConfigurationChildBuilder implements Builder<RecoveryConfiguration>, ConfigurationBuilderInfo {

   private static final Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass());
   private final AttributeSet attributes;

   RecoveryConfigurationBuilder(TransactionConfigurationBuilder builder) {
      super(builder);
      attributes = RecoveryConfiguration.attributeDefinitionSet();
   }

   @Override
   public AttributeSet attributes() {
      return attributes;
   }

   @Override
   public ElementDefinition getElementDefinition() {
      return RecoveryConfiguration.ELEMENT_DEFINITION;
   }

   /**
    * Enable recovery for this cache
    */
   public RecoveryConfigurationBuilder enable() {
      attributes.attribute(ENABLED).set(true);
      return this;
   }

   /**
    * Disable recovery for this cache
    */
   public RecoveryConfigurationBuilder disable() {
      attributes.attribute(ENABLED).set(false);
      return this;
   }

   /**
    * Enable recovery for this cache
    */
   public RecoveryConfigurationBuilder enabled(boolean enabled) {
      attributes.attribute(ENABLED).set(enabled);
      return this;
   }

   boolean isEnabled() {
      return attributes.attribute(ENABLED).get();
   }

   /**
    * Sets the name of the cache where recovery related information is held. If not specified
    * defaults to a cache named {@link RecoveryConfiguration#DEFAULT_RECOVERY_INFO_CACHE}
    */
   public RecoveryConfigurationBuilder recoveryInfoCacheName(String recoveryInfoName) {
      attributes.attribute(RECOVERY_INFO_CACHE_NAME).set(recoveryInfoName);
      return this;
   }

   @Override
   public void validate() {
      if (!attributes.attribute(ENABLED).get()) {
         return; //nothing to validate
      }
      if (transaction().transactionMode() == TransactionMode.NON_TRANSACTIONAL) {
         throw log.recoveryNotSupportedWithNonTxCache();
      }
      if (transaction().useSynchronization()) {
         throw log.recoveryNotSupportedWithSynchronization();
      }
      if (transaction().transactionProtocol() == TransactionProtocol.TOTAL_ORDER) {
         throw log.unavailableTotalOrderWithTxRecovery();
      }
   }

   @Override
   public void validate(GlobalConfiguration globalConfig) {
      validate();
   }

   @Override
   public RecoveryConfiguration create() {
      return new RecoveryConfiguration(attributes.protect());
   }

   @Override
   public RecoveryConfigurationBuilder read(RecoveryConfiguration template) {
      this.attributes.read(template.attributes());

      return this;
   }

   @Override
   public String toString() {
      return "RecoveryConfigurationBuilder [attributes=" + attributes + "]";
   }


}

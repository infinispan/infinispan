package org.infinispan.configuration.cache;

import static org.infinispan.configuration.cache.RecoveryConfiguration.ENABLED;
import static org.infinispan.configuration.cache.RecoveryConfiguration.RECOVERY_INFO_CACHE_NAME;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.configuration.global.GlobalConfiguration;

/**
 * Defines recovery configuration for the cache.
 *
 * @author pmuir
 *
 */
public class RecoveryConfigurationBuilder extends AbstractTransportConfigurationChildBuilder implements Builder<RecoveryConfiguration> {

   private final AttributeSet attributes;

   RecoveryConfigurationBuilder(TransactionConfigurationBuilder builder) {
      super(builder);
      attributes = RecoveryConfiguration.attributeDefinitionSet();
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
      if (!attributes.attribute(ENABLED).get() || transaction().useSynchronization()) {
         return;
      }
      if (!clustering().cacheMode().isSynchronous()) {
         throw new CacheConfigurationException("Recovery not supported with Asynchronous " +
                                                clustering().cacheMode().friendlyCacheModeString() + " cache mode.");
      }
      if (!transaction().syncCommitPhase()) {
         //configuration not supported because the Transaction Manager would not retain any transaction log information to
         //allow it to perform useful recovery anyhow. Usually you just log it in the hope a human notices and sorts
         //out the mess. Of course properly paranoid humans don't use async commit in the first place.
         throw new CacheConfigurationException("Recovery not supported with asynchronous commit phase.");
      }
   }

   @Override
   public void validate(GlobalConfiguration globalConfig) {
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

package org.infinispan.configuration.cache;

import static org.infinispan.configuration.cache.StoreAsBinaryConfiguration.ENABLED;
import static org.infinispan.configuration.cache.StoreAsBinaryConfiguration.STORE_KEYS_AS_BINARY;
import static org.infinispan.configuration.cache.StoreAsBinaryConfiguration.STORE_VALUES_AS_BINARY;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.configuration.global.GlobalConfiguration;

/**
 * Controls whether when stored in memory, keys and values are stored as references to their original objects, or in
 * a serialized, binary format.  There are benefits to both approaches, but often if used in a clustered mode,
 * storing objects as binary means that the cost of serialization happens early on, and can be amortized.  Further,
 * deserialization costs are incurred lazily which improves throughput.
 * <p />
 * It is possible to control this on a fine-grained basis: you can choose to just store keys or values as binary, or
 * both.
 * <p />
 * @see StoreAsBinaryConfiguration
 */
public class StoreAsBinaryConfigurationBuilder extends AbstractConfigurationChildBuilder implements Builder<StoreAsBinaryConfiguration> {
   private final AttributeSet attributes;

   StoreAsBinaryConfigurationBuilder(ConfigurationBuilder builder) {
      super(builder);
      this.attributes = StoreAsBinaryConfiguration.attributeDefinitionSet();
   }

   /**
    * Enables storing both keys and values as binary.
    */
   public StoreAsBinaryConfigurationBuilder enable() {
      attributes.attribute(ENABLED).set(true);
      return this;
   }

   /**
    * Disables storing both keys and values as binary.
    */
   public StoreAsBinaryConfigurationBuilder disable() {
      attributes.attribute(ENABLED).set(false);
      return this;
   }

   /**
    * Sets whether this feature is enabled or disabled.
    * @param enabled if true, this feature is enabled.  If false, it is disabled.
    */
   public StoreAsBinaryConfigurationBuilder enabled(boolean enabled) {
      attributes.attribute(ENABLED).set(enabled);
      return this;
   }

   /**
    * Specify whether keys are stored as binary or not.
    * @param storeKeysAsBinary if true, keys are stored as binary.  If false, keys are stored as object references.
    */
   public StoreAsBinaryConfigurationBuilder storeKeysAsBinary(boolean storeKeysAsBinary) {
      attributes.attribute(STORE_KEYS_AS_BINARY).set(storeKeysAsBinary);
      return this;
   }
   /**
    * Specify whether values are stored as binary or not.
    * @param storeValuesAsBinary if true, values are stored as binary.  If false, values are stored as object references.
    */
   public StoreAsBinaryConfigurationBuilder storeValuesAsBinary(boolean storeValuesAsBinary) {
      attributes.attribute(STORE_VALUES_AS_BINARY).set(storeValuesAsBinary);
      return this;
   }

   /**
    * When defensive copying is disabled, Infinispan keeps object references
    * around and marshalls keys lazily. So clients can modify entries via
    * original object references, and marshalling only happens when entries
    * are to be replicated/distributed, or stored in a cache store.
    *
    * Since client references are valid, clients can make changes to entries
    * in the cache using those references, but these modifications are only
    * local and you still need to call one of the cache's put/replace...
    * methods in order for changes to replicate.
    *
    * When defensive copies are enabled, Infinispan marshalls objects the
    * moment they're stored, hence changes made to object references are
    * not stored in the cache, not even for local caches.
    *
    * @param defensive boolean indicating whether defensive copies
    *                  should be enabled cache wide
    * @return a configuration builder for fluent programmatic configuration
    * @deprecated Store as binary configuration is always defensive now.
    */
   @Deprecated
   public StoreAsBinaryConfigurationBuilder defensive(boolean defensive) {
      return this;
   }

   @Override
   public void validate() {
      // Nothing to validate.
   }

   @Override
   public void validate(GlobalConfiguration globalConfig) {
   }

   @Override
   public StoreAsBinaryConfiguration create() {
      return new StoreAsBinaryConfiguration(attributes.protect());
   }

   @Override
   public StoreAsBinaryConfigurationBuilder read(StoreAsBinaryConfiguration template) {
      this.attributes.read(template.attributes());
      return this;
   }

   @Override
   public String toString() {
      return "StoreAsBinaryConfigurationBuilder [attributes=" + attributes + "]";
   }
}

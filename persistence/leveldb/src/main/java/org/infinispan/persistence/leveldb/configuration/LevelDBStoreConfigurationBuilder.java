package org.infinispan.persistence.leveldb.configuration;

import static org.infinispan.persistence.leveldb.configuration.LevelDBStoreConfiguration.IMPLEMENTATION_TYPE;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.configuration.cache.AbstractStoreConfigurationBuilder;
import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;
/**
 *
 * @author <a href="mailto:rtsang@redhat.com">Ray Tsang</a>
 * @deprecated Use the RocksDB Cache Store instead
 */
@Deprecated
public class LevelDBStoreConfigurationBuilder extends AbstractStoreConfigurationBuilder<LevelDBStoreConfiguration, LevelDBStoreConfigurationBuilder> {

   public LevelDBStoreConfigurationBuilder(PersistenceConfigurationBuilder builder) {
      super(builder, LevelDBStoreConfiguration.attributeDefinitionSet());
   }

   public LevelDBStoreConfigurationBuilder location(String location) {
      attributes.attribute("location").set(location);
      return self();
   }

   public LevelDBStoreConfigurationBuilder expiredLocation(String expiredLocation) {
      attributes.attribute("expiredLocation").set(expiredLocation);
      return self();
   }

   public LevelDBStoreConfigurationBuilder implementationType(LevelDBStoreConfiguration.ImplementationType implementationType) {
      attributes.attribute(IMPLEMENTATION_TYPE).set(implementationType);
      return self();
   }

   public LevelDBStoreConfigurationBuilder blockSize(int blockSize) {
      attributes.attribute("blockSize").set(blockSize);
      return self();
   }

   public LevelDBStoreConfigurationBuilder cacheSize(long cacheSize) {
      attributes.attribute("cacheSize").set(cacheSize);
      return self();
   }

   public LevelDBStoreConfigurationBuilder expiryQueueSize(int expiryQueueSize) {
      attributes.attribute("expiryQueueSize").set(expiryQueueSize);
      return self();
   }

   public LevelDBStoreConfigurationBuilder clearThreshold(int clearThreshold) {
      attributes.attribute("clearThreshold").set(clearThreshold);
      return self();
   }

   public LevelDBStoreConfigurationBuilder compressionType(CompressionType compressionType) {
      attributes.attribute("compressionType").set(compressionType);
      return self();
   }

   @Override
   public void validate() {
      // how do you validate required attributes?
      super.validate();
   }

   @Override
   public LevelDBStoreConfiguration create() {
      return new LevelDBStoreConfiguration(attributes.protect(), async.create(), singletonStore.create());
   }

   @Override
   public Builder<?> read(LevelDBStoreConfiguration template) {
      super.read(template);
      return self();
   }

   @Override
   public LevelDBStoreConfigurationBuilder self() {
      return this;
   }

}

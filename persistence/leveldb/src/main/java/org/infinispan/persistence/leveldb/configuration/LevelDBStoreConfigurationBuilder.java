package org.infinispan.persistence.leveldb.configuration;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.configuration.cache.AbstractStoreConfigurationBuilder;
import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;

/**
 *
 * @author <a href="mailto:rtsang@redhat.com">Ray Tsang</a>
 *
 */
public class LevelDBStoreConfigurationBuilder extends AbstractStoreConfigurationBuilder<LevelDBStoreConfiguration, LevelDBStoreConfigurationBuilder> {

   protected String location = "Infinispan-LevelDBStore/data";
   protected String expiredLocation = "Infinispan-LevelDBStore/expired";
   protected CompressionType compressionType = CompressionType.NONE;
   protected LevelDBStoreConfiguration.ImplementationType implementationType = LevelDBStoreConfiguration.ImplementationType.AUTO;
   protected Integer blockSize;
   protected Long cacheSize;

   protected int expiryQueueSize = 10000;
   protected int clearThreshold = 10000;

   public LevelDBStoreConfigurationBuilder(PersistenceConfigurationBuilder builder) {
      super(builder);
   }

   public LevelDBStoreConfigurationBuilder location(String location) {
      this.location = location;
      return self();
   }

   public LevelDBStoreConfigurationBuilder expiredLocation(String expiredLocation) {
      this.expiredLocation = expiredLocation;
      return self();
   }

   public LevelDBStoreConfigurationBuilder implementationType(LevelDBStoreConfiguration.ImplementationType implementationType) {
      this.implementationType = implementationType;
      return self();
   }

   public LevelDBStoreConfigurationBuilder blockSize(int blockSize) {
      this.blockSize = blockSize;
      return self();
   }

   public LevelDBStoreConfigurationBuilder cacheSize(long cacheSize) {
      this.cacheSize = cacheSize;
      return self();
   }

   public LevelDBStoreConfigurationBuilder expiryQueueSize(int expiryQueueSize) {
      this.expiryQueueSize = expiryQueueSize;
      return self();
   }

   public LevelDBStoreConfigurationBuilder clearThreshold(int clearThreshold) {
      this.clearThreshold = clearThreshold;
      return self();
   }

   public LevelDBStoreConfigurationBuilder compressionType(CompressionType compressionType) {
      this.compressionType = compressionType;
      return self();
   }

   @Override
   public void validate() {
      // how do you validate required attributes?
      super.validate();
   }

   @Override
   public LevelDBStoreConfiguration create() {
      return new LevelDBStoreConfiguration(purgeOnStartup, fetchPersistentState, ignoreModifications, async.create(),
                                                singletonStore.create(), preload, shared, properties,location,
                                                expiredLocation, implementationType, compressionType,  blockSize,
                                                cacheSize, expiryQueueSize, clearThreshold);
   }

   @Override
   public Builder<?> read(LevelDBStoreConfiguration template) {
      location = template.location();
      expiredLocation = template.expiredLocation();
      implementationType = template.implementationType();
      preload = template.preload();
      shared = template.shared();

      compressionType = template.compressionType();
      blockSize = template.blockSize();
      cacheSize = template.cacheSize();

      expiryQueueSize = template.expiryQueueSize();
      clearThreshold = template.clearThreshold();


      // AbstractStore-specific configuration
      fetchPersistentState = template.fetchPersistentState();
      ignoreModifications = template.ignoreModifications();
      properties = template.properties();
      purgeOnStartup = template.purgeOnStartup();
      this.async.read(template.async());
      this.singletonStore.read(template.singletonStore());

      return self();
   }

   @Override
   public LevelDBStoreConfigurationBuilder self() {
      return this;
   }

}

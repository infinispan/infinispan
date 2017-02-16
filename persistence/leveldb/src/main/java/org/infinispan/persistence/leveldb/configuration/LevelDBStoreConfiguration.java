package org.infinispan.persistence.leveldb.configuration;

import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.commons.configuration.ConfigurationFor;
import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.configuration.cache.AsyncStoreConfiguration;
import org.infinispan.configuration.cache.SingletonStoreConfiguration;
import org.infinispan.configuration.serializing.SerializedWith;
import org.infinispan.persistence.leveldb.LevelDBStore;
import org.infinispan.persistence.rocksdb.configuration.RocksDBStoreConfiguration;

/**
 *
 * @author <a href="mailto:rtsang@redhat.com">Ray Tsang</a>
 * @deprecated Use the RocksDB cache store instead
 */
@Deprecated
@ConfigurationFor(LevelDBStore.class)
@BuiltBy(LevelDBStoreConfigurationBuilder.class)
@SerializedWith(LevelDBStoreConfigurationSerializer.class)
public class LevelDBStoreConfiguration extends RocksDBStoreConfiguration {
   public enum ImplementationType {
      AUTO, JAVA, JNI
   }

   final static AttributeDefinition<ImplementationType> IMPLEMENTATION_TYPE = AttributeDefinition.builder("implementationType", ImplementationType.AUTO).immutable().autoPersist(false).build();

   public static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(LevelDBStoreConfiguration.class, RocksDBStoreConfiguration.attributeDefinitionSet(), IMPLEMENTATION_TYPE);
   }

   private final Attribute<ImplementationType> implementationType;

   public LevelDBStoreConfiguration(AttributeSet attributes, AsyncStoreConfiguration async, SingletonStoreConfiguration singletonStore) {
      super(attributes, async, singletonStore);
      implementationType = attributes.attribute(IMPLEMENTATION_TYPE);
   }


   public ImplementationType implementationType() {
      return implementationType.get();
   }

}

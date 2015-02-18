package org.infinispan.persistence.jdbc.configuration;

import org.infinispan.configuration.cache.AsyncStoreConfiguration;
import org.infinispan.configuration.cache.SingletonStoreConfiguration;
import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.commons.configuration.ConfigurationFor;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.persistence.jdbc.DatabaseType;
import org.infinispan.persistence.jdbc.binary.JdbcBinaryStore;

import java.util.Properties;

@BuiltBy(JdbcBinaryStoreConfigurationBuilder.class)
@ConfigurationFor(JdbcBinaryStore.class)
public class JdbcBinaryStoreConfiguration extends AbstractJdbcStoreConfiguration {
   static final AttributeDefinition<Integer> CONCURRENCY_LEVEL = AttributeDefinition.builder("concurrencyLevel", 2048).immutable().build();
   static final AttributeDefinition<Long> LOCK_ACQUISITION_TIMEOUT = AttributeDefinition.builder("lockAcquisitionTimeout", 60000l).immutable().build();
   private final TableManipulationConfiguration table;
   public static AttributeSet attributeSet() {
      return new AttributeSet(JdbcBinaryStoreConfiguration.class, AbstractJdbcStoreConfiguration.attributeSet(), CONCURRENCY_LEVEL, LOCK_ACQUISITION_TIMEOUT);
   }

   public JdbcBinaryStoreConfiguration(AttributeSet attributes, AsyncStoreConfiguration async, SingletonStoreConfiguration singletonStore, ConnectionFactoryConfiguration connectionFactory, TableManipulationConfiguration table) {
      super(attributes, async, singletonStore, connectionFactory);
      this.table = table;
   }

   public TableManipulationConfiguration table() {
      return table;
   }

   public int lockConcurrencyLevel() {
      return attributes.attribute(CONCURRENCY_LEVEL).asInteger();
   }

   public long lockAcquisitionTimeout() {
      return attributes.attribute(LOCK_ACQUISITION_TIMEOUT).asLong();
   }

   @Override
   public String toString() {
      return "JdbcBinaryStoreConfiguration [table=" + table + ", attributes=" + attributes + ", connectionFactory=" + connectionFactory() + "]";
   }

   @Override
   public int hashCode() {
      final int prime = 31;
      int result = super.hashCode();
      result = prime * result + ((table == null) ? 0 : table.hashCode());
      return result;
   }

   @Override
   public boolean equals(Object obj) {
      if (this == obj)
         return true;
      if (!super.equals(obj))
         return false;
      if (getClass() != obj.getClass())
         return false;
      JdbcBinaryStoreConfiguration other = (JdbcBinaryStoreConfiguration) obj;
      if (table == null) {
         if (other.table != null)
            return false;
      } else if (!table.equals(other.table))
         return false;
      return true;
   }

}

package org.infinispan.persistence.jdbc.common.configuration;

import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.configuration.cache.AbstractStoreConfiguration;
import org.infinispan.configuration.cache.AsyncStoreConfiguration;
import org.infinispan.persistence.jdbc.common.DatabaseType;

public abstract class AbstractJdbcStoreConfiguration extends AbstractStoreConfiguration {
   static final AttributeDefinition<DatabaseType> DIALECT = AttributeDefinition.builder(org.infinispan.persistence.jdbc.common.configuration.Attribute.DIALECT, null, DatabaseType.class).immutable().build();
   static final AttributeDefinition<Integer> READ_QUERY_TIMEOUT = AttributeDefinition.builder(org.infinispan.persistence.jdbc.common.configuration.Attribute.READ_QUERY_TIMEOUT, 0, Integer.class).build();
   static final AttributeDefinition<Integer> WRITE_QUERY_TIMEOUT = AttributeDefinition.builder(org.infinispan.persistence.jdbc.common.configuration.Attribute.WRITE_QUERY_TIMEOUT, 0, Integer.class).build();

   public static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(AbstractJdbcStoreConfiguration.class, AbstractStoreConfiguration.attributeDefinitionSet(),
            DIALECT, READ_QUERY_TIMEOUT, WRITE_QUERY_TIMEOUT);
   }

   private final Attribute<DatabaseType> dialect;
   private final Attribute<Integer> readQueryTimeout;
   private final Attribute<Integer> writeQueryTimeout;
   private final ConnectionFactoryConfiguration connectionFactory;

   protected AbstractJdbcStoreConfiguration(AttributeSet attributes, AsyncStoreConfiguration async, ConnectionFactoryConfiguration connectionFactory) {
      super(attributes, async);
      this.connectionFactory = connectionFactory;
      dialect = attributes.attribute(DIALECT);
      readQueryTimeout = attributes.attribute(READ_QUERY_TIMEOUT);
      writeQueryTimeout = attributes.attribute(WRITE_QUERY_TIMEOUT);
   }

   public ConnectionFactoryConfiguration connectionFactory() {
      return connectionFactory;
   }

   /**
    * @return always returns false
    * @deprecated Since 13.0 with no replacement
    */
   public boolean manageConnectionFactory() {
      return false;
   }

   public DatabaseType dialect() {
      return dialect.get();
   }

   /**
    * @deprecated since 14.0, always returns <b>null</b>
    */
   public Integer dbMajorVersion() {
      return null;
   }

   /**
    * @deprecated since 14.0, always returns <b>null</b>
    */
   @Deprecated
   public Integer dbMinorVersion() {
      return null;
   }

   public Integer readQueryTimeout() {
      return readQueryTimeout.get();
   }

   public Integer writeQueryTimeout() {
      return writeQueryTimeout.get();
   }

   @Override
   public String toString() {
      return "AbstractJdbcStoreConfiguration [connectionFactory=" + connectionFactory + ", attributes=" + attributes + "]";
   }

   @Override
   public int hashCode() {
      final int prime = 31;
      int result = super.hashCode();
      result = prime * result + ((connectionFactory == null) ? 0 : connectionFactory.hashCode());
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
      AbstractJdbcStoreConfiguration other = (AbstractJdbcStoreConfiguration) obj;
      if (connectionFactory == null) {
         if (other.connectionFactory != null)
            return false;
      } else if (!connectionFactory.equals(other.connectionFactory))
         return false;
      return true;
   }

}

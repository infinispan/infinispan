package org.infinispan.persistence.jdbc.configuration;

import java.util.ArrayList;
import java.util.List;

import org.infinispan.commons.configuration.ConfigurationInfo;
import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.configuration.cache.AbstractStoreConfiguration;
import org.infinispan.configuration.cache.AsyncStoreConfiguration;
import org.infinispan.configuration.cache.SingletonStoreConfiguration;
import org.infinispan.persistence.jdbc.DatabaseType;

public abstract class AbstractJdbcStoreConfiguration extends AbstractStoreConfiguration implements ConfigurationInfo {
   static final AttributeDefinition<Boolean> MANAGE_CONNECTION_FACTORY = AttributeDefinition.builder("manageConnectionFactory", true).immutable().build();
   static final AttributeDefinition<DatabaseType> DIALECT = AttributeDefinition.builder("databaseType", null, DatabaseType.class).immutable().xmlName("dialect").build();
   static final AttributeDefinition<Integer> DB_MAJOR_VERSION = AttributeDefinition.builder("databaseMajorVersion", null, Integer.class).immutable().xmlName("db-major-version").build();
   static final AttributeDefinition<Integer> DB_MINOR_VERSION = AttributeDefinition.builder("databaseMinorVersion", null, Integer.class).immutable().xmlName("db-minor-version").build();

   public static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(AbstractJdbcStoreConfiguration.class, AbstractStoreConfiguration.attributeDefinitionSet(),
                              MANAGE_CONNECTION_FACTORY, DIALECT, DB_MAJOR_VERSION, DB_MINOR_VERSION);
   }

   private final Attribute<Boolean> manageConnectionFactory;
   private final Attribute<DatabaseType> dialect;
   private final Attribute<Integer> dbMajorVersion;
   private final Attribute<Integer> dbMinorVersion;
   private final ConnectionFactoryConfiguration connectionFactory;

   private final List<ConfigurationInfo> subElements;

   protected AbstractJdbcStoreConfiguration(AttributeSet attributes, AsyncStoreConfiguration async, SingletonStoreConfiguration singletonStore, ConnectionFactoryConfiguration connectionFactory) {
      super(attributes, async, singletonStore);
      this.connectionFactory = connectionFactory;
      manageConnectionFactory = attributes.attribute(MANAGE_CONNECTION_FACTORY);
      dialect = attributes.attribute(DIALECT);
      dbMajorVersion = attributes.attribute(DB_MAJOR_VERSION);
      dbMinorVersion = attributes.attribute(DB_MINOR_VERSION);
      subElements = new ArrayList<>(super.subElements());
      subElements.add(connectionFactory);
   }

   @Override
   public List<ConfigurationInfo> subElements() {
      return subElements;
   }

   public ConnectionFactoryConfiguration connectionFactory() {
      return connectionFactory;
   }

   public boolean manageConnectionFactory() {
      return manageConnectionFactory.get();
   }

   public DatabaseType dialect() {
      return dialect.get();
   }

   public Integer dbMajorVersion() {
      return dbMajorVersion.get();
   }

   public Integer dbMinorVersion() {
      return dbMinorVersion.get();
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

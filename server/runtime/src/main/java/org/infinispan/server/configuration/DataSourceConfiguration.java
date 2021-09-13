package org.infinispan.server.configuration;

import static org.infinispan.commons.configuration.attributes.AttributeSerializer.SECRET;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Supplier;

import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.ConfigurationElement;

import io.agroal.api.configuration.AgroalConnectionFactoryConfiguration;
import io.agroal.api.configuration.AgroalConnectionFactoryConfiguration.TransactionIsolation;

public class DataSourceConfiguration extends ConfigurationElement<DataSourceConfiguration> {

   static final AttributeDefinition<String> NAME = AttributeDefinition.builder(Attribute.NAME, null, String.class).build();
   static final AttributeDefinition<String> JNDI_NAME = AttributeDefinition.builder(Attribute.JNDI_NAME, null, String.class).build();
   static final AttributeDefinition<Boolean> STATISTICS = AttributeDefinition.builder(Attribute.STATISTICS, false, Boolean.class).build();

   static final AttributeDefinition<String> DRIVER = AttributeDefinition.builder(Attribute.DRIVER, null, String.class).build();
   static final AttributeDefinition<String> URL = AttributeDefinition.builder(Attribute.URL, null, String.class).build();
   static final AttributeDefinition<String> USERNAME = AttributeDefinition.builder(Attribute.USERNAME, null, String.class).build();
   static final AttributeDefinition<Supplier<char[]>> PASSWORD = AttributeDefinition.builder(Attribute.PASSWORD, null, (Class<Supplier<char[]>>) (Class<?>) Supplier.class).serializer(SECRET).build();
   static final AttributeDefinition<String> INITIAL_SQL = AttributeDefinition.builder(Attribute.NEW_CONNECTION_SQL, null, String.class).build();
   static final AttributeDefinition<TransactionIsolation> TRANSACTION_ISOLATION = AttributeDefinition.builder(Attribute.TRANSACTION_ISOLATION, TransactionIsolation.READ_COMMITTED, AgroalConnectionFactoryConfiguration.TransactionIsolation.class).build();

   static final AttributeDefinition<Integer> MAX_SIZE = AttributeDefinition.builder(Attribute.MAX_SIZE, null, Integer.class).build();
   static final AttributeDefinition<Integer> MIN_SIZE = AttributeDefinition.builder(Attribute.MIN_SIZE, 0, Integer.class).build();
   static final AttributeDefinition<Integer> INITIAL_SIZE = AttributeDefinition.builder(Attribute.INITIAL_SIZE, 0, Integer.class).build();

   static final AttributeDefinition<Long> BLOCKING_TIMEOUT = AttributeDefinition.builder(Attribute.BLOCKING_TIMEOUT, 0L, Long.class).build();
   static final AttributeDefinition<Long> BACKGROUND_VALIDATION = AttributeDefinition.builder(Attribute.BACKGROUND_VALIDATION, 0L, Long.class).build();
   static final AttributeDefinition<Long> VALIDATE_ON_ACQUISITION = AttributeDefinition.builder(Attribute.VALIDATE_ON_ACQUISITION, 0L, Long.class).build();
   static final AttributeDefinition<Long> LEAK_DETECTION = AttributeDefinition.builder(Attribute.LEAK_DETECTION, 0L, Long.class).build();
   static final AttributeDefinition<Integer> IDLE_REMOVAL = AttributeDefinition.builder(Attribute.IDLE_REMOVAL, 0, Integer.class).build();

   static final AttributeDefinition<Map<String, String>> CONNECTION_PROPERTIES = AttributeDefinition.builder(Element.CONNECTION_PROPERTY, null, (Class<Map<String, String>>) (Class<?>) Map.class).initializer(LinkedHashMap::new).autoPersist(false).immutable().build();

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(DataSourceConfiguration.class, NAME, JNDI_NAME, STATISTICS, DRIVER, URL,
            USERNAME, PASSWORD, INITIAL_SQL, TRANSACTION_ISOLATION, MAX_SIZE, MIN_SIZE, INITIAL_SIZE,
            BLOCKING_TIMEOUT, BACKGROUND_VALIDATION, VALIDATE_ON_ACQUISITION, LEAK_DETECTION, IDLE_REMOVAL,
            CONNECTION_PROPERTIES);
   }

   DataSourceConfiguration(AttributeSet attributes) {
      super(Element.DATA_SOURCE, attributes);
   }

   public String name() {
      return attributes.attribute(NAME).get();
   }

   public String jndiName() {
      return attributes.attribute(JNDI_NAME).get();
   }

   public Boolean statistics() {
      return attributes.attribute(STATISTICS).get();
   }

   public String driver() {
      return attributes.attribute(DRIVER).get();
   }

   public String username() {
      return attributes.attribute(USERNAME).get();
   }

   public char[] password() {
      return attributes.attribute(PASSWORD).get().get();
   }

   public String url() {
      return attributes.attribute(URL).get();
   }

   public TransactionIsolation transactionIsolation() {
      return attributes.attribute(TRANSACTION_ISOLATION).get();
   }

   public String initialSql() {
      return attributes.attribute(INITIAL_SQL).get();
   }

   public int maxSize() {
      return attributes.attribute(MAX_SIZE).get();
   }

   public int minSize() {
      return attributes.attribute(MIN_SIZE).get();
   }

   public int initialSize() {
      return attributes.attribute(INITIAL_SIZE).get();
   }

   public long blockingTimeout() {
      return attributes.attribute(BLOCKING_TIMEOUT).get();
   }

   public long backgroundValidation() {
      return attributes.attribute(BACKGROUND_VALIDATION).get();
   }

   public long validateOnAcquisition() {
      return attributes.attribute(VALIDATE_ON_ACQUISITION).get();
   }

   public long leakDetection() {
      return attributes.attribute(LEAK_DETECTION).get();
   }

   public int idleRemoval() {
      return attributes.attribute(IDLE_REMOVAL).get();
   }

   public Map<String, String> connectionProperties() {
      return attributes.attribute(CONNECTION_PROPERTIES).get();
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      DataSourceConfiguration that = (DataSourceConfiguration) o;

      return attributes.equals(that.attributes);
   }

   @Override
   public int hashCode() {
      return attributes.hashCode();
   }

   @Override
   public String toString() {
      return "DataSourceConfiguration{" +
            "attributes=" + attributes +
            '}';
   }
}

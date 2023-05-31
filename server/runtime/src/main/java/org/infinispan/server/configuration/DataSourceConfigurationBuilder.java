package org.infinispan.server.configuration;

import java.util.Map;
import java.util.function.Supplier;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.Combine;
import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.server.security.PasswordCredentialSource;
import org.wildfly.security.credential.source.CredentialSource;

import io.agroal.api.configuration.AgroalConnectionFactoryConfiguration;

public class DataSourceConfigurationBuilder implements Builder<DataSourceConfiguration> {

   private final AttributeSet attributes;

   DataSourceConfigurationBuilder(String name, String jndiName) {
      attributes = DataSourceConfiguration.attributeDefinitionSet();
      attributes.attribute(DataSourceConfiguration.NAME).set(name);
      attributes.attribute(DataSourceConfiguration.JNDI_NAME).set(jndiName);
   }

   @Override
   public AttributeSet attributes() {
      return attributes;
   }

   @Override
   public DataSourceConfiguration create() {
      return new DataSourceConfiguration(attributes.protect());
   }

   @Override
   public DataSourceConfigurationBuilder read(DataSourceConfiguration template, Combine combine) {
      this.attributes.read(template.attributes(), combine);
      return this;
   }

   public DataSourceConfigurationBuilder driver(String driver) {
      attributes.attribute(DataSourceConfiguration.DRIVER).set(driver);
      return this;
   }

   public DataSourceConfigurationBuilder username(String username) {
      attributes.attribute(DataSourceConfiguration.USERNAME).set(username);
      return this;
   }

   public DataSourceConfigurationBuilder password(char[] password) {
      attributes.attribute(DataSourceConfiguration.PASSWORD).set(new PasswordCredentialSource(password));
      return this;
   }

   public DataSourceConfigurationBuilder password(Supplier<CredentialSource> password) {
      attributes.attribute(DataSourceConfiguration.PASSWORD).set(password);
      return this;
   }

   public DataSourceConfigurationBuilder url(String url) {
      attributes.attribute(DataSourceConfiguration.URL).set(url);
      return this;
   }

   public DataSourceConfigurationBuilder transactionIsolation(AgroalConnectionFactoryConfiguration.TransactionIsolation transactionIsolation) {
      attributes.attribute(DataSourceConfiguration.TRANSACTION_ISOLATION).set(transactionIsolation);
      return this;
   }

   public DataSourceConfigurationBuilder newConnectionSql(String newConnectionSql) {
      attributes.attribute(DataSourceConfiguration.INITIAL_SQL).set(newConnectionSql);
      return this;
   }

   public DataSourceConfigurationBuilder maxSize(int maxSize) {
      attributes.attribute(DataSourceConfiguration.MAX_SIZE).set(maxSize);
      return this;
   }

   public DataSourceConfigurationBuilder minSize(int minSize) {
      attributes.attribute(DataSourceConfiguration.MIN_SIZE).set(minSize);
      return this;
   }

   public DataSourceConfigurationBuilder initialSize(int initialSize) {
      attributes.attribute(DataSourceConfiguration.INITIAL_SIZE).set(initialSize);
      return this;
   }

   public DataSourceConfigurationBuilder blockingTimeout(long blockingTimeout) {
      attributes.attribute(DataSourceConfiguration.BLOCKING_TIMEOUT).set(blockingTimeout);
      return this;
   }

   public DataSourceConfigurationBuilder backgroundValidation(long backgroundValidation) {
      attributes.attribute(DataSourceConfiguration.BACKGROUND_VALIDATION).set(backgroundValidation);
      return this;
   }

   public DataSourceConfigurationBuilder validateOnAcquisition(long validateOnAcquisition) {
      attributes.attribute(DataSourceConfiguration.VALIDATE_ON_ACQUISITION).set(validateOnAcquisition);
      return this;
   }

   public DataSourceConfigurationBuilder leakDetection(long leakDetection) {
      attributes.attribute(DataSourceConfiguration.LEAK_DETECTION).set(leakDetection);
      return this;
   }

   public DataSourceConfigurationBuilder idleRemoval(int idleRemoval) {
      attributes.attribute(DataSourceConfiguration.IDLE_REMOVAL).set(idleRemoval);
      return this;
   }

   public DataSourceConfigurationBuilder statistics(boolean enable) {
      attributes.attribute(DataSourceConfiguration.STATISTICS).set(enable);
      return this;
   }

   public DataSourceConfigurationBuilder addProperty(String key, String value) {
      Attribute<Map<String, String>> a = attributes.attribute(DataSourceConfiguration.CONNECTION_PROPERTIES);
      Map<String, String> map = a.get();
      map.put(key, value);
      a.set(map);
      return this;
   }
}

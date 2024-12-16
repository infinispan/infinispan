package org.infinispan.cdc.configuration;

import java.util.Properties;
import java.util.Set;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.Combine;
import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.persistence.jdbc.common.configuration.AbstractJdbcStoreConfigurationBuilder;

public final class ChangeDataCaptureConfigurationBuilder extends AbstractJdbcStoreConfigurationBuilder<ChangeDataCaptureConfiguration, ChangeDataCaptureConfigurationBuilder> {

   private final TableConfigurationBuilder tcb;
   private final Properties connectorProperties;

   public ChangeDataCaptureConfigurationBuilder(ConfigurationBuilder builder) {
      super(builder.persistence(), ChangeDataCaptureConfiguration.attributeSet());
      this.tcb = new TableConfigurationBuilder(this);
      this.connectorProperties = new Properties();
   }

   public ChangeDataCaptureConfigurationBuilder enabled(boolean enabled) {
      attributes.attribute(ChangeDataCaptureConfiguration.ENABLED).set(enabled);
      return this;
   }

   public ChangeDataCaptureConfigurationBuilder addForeignKey(String fk) {
      Attribute<Set<String>> attribute = attributes.attribute(ChangeDataCaptureConfiguration.FOREIGN_KEYS);
      Set<String> fks = attribute.get();
      fks.add(fk);
      attribute.set(fks);
      return this;
   }

   @Override
   public ChangeDataCaptureConfiguration create() {
      return new ChangeDataCaptureConfiguration(ChangeDataCaptureConfiguration.protectAttributeSet(attributes), connectionFactory.create(), tcb.create(), connectorProperties);
   }

   @Override
   public Builder<?> read(ChangeDataCaptureConfiguration template, Combine combine) {
      super.read(template, combine);
      tcb.read(template.table(), combine);
      return this;
   }

   @Override
   public ChangeDataCaptureConfigurationBuilder self() {
      return this;
   }

   public TableConfigurationBuilder table() {
      return tcb;
   }

   public Properties connectorProperties() {
      return connectorProperties;
   }

   @Override
   public void validate() {
      tcb.validate();
      super.validate();
   }
}

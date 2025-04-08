package org.infinispan.cdc.configuration;

import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import org.infinispan.cdc.ChangeDataCaptureManager;
import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.commons.configuration.ConfigurationFor;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSerializer;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.util.Experimental;
import org.infinispan.configuration.serializing.SerializedWith;
import org.infinispan.persistence.jdbc.common.configuration.AbstractJdbcStoreConfiguration;
import org.infinispan.persistence.jdbc.common.configuration.ConnectionFactoryConfiguration;

/**
 * Configuration to enable change data capture in a cache.
 *
 * <p>
 * This is the complete, user-facing configuration, to enable CDC directly attached to a cache. The configuration holds
 * the database configuration and the foreign keys for expansion. Most parameters inherited from
 * {@link AbstractJdbcStoreConfiguration} are ignored as they are not needed for CDC.
 * </p>
 *
 * @since 16.0
 */
@Experimental
@ConfigurationFor(ChangeDataCaptureManager.class)
@SerializedWith(CDCConfigurationSerializer.class)
@BuiltBy(ChangeDataCaptureConfigurationBuilder.class)
public class ChangeDataCaptureConfiguration extends AbstractJdbcStoreConfiguration<ChangeDataCaptureConfiguration> {

   static final AttributeDefinition<Boolean> ENABLED = AttributeDefinition
         .builder(Attribute.ENABLED, false)
         .immutable()
         .build();

   @SuppressWarnings("unchecked")
   static final AttributeDefinition<Set<String>> FOREIGN_KEYS = AttributeDefinition
         .builder(Attribute.FOREIGN_KEYS, null, (Class<Set<String>>) (Class<?>) Set.class)
         .initializer(HashSet::new)
         .serializer(AttributeSerializer.STRING_COLLECTION)
         .immutable()
         .build();

   private final AttributeSet attributes;
   private final TableConfiguration tableConfiguration;
   private final Properties connectorProperties;

   public ChangeDataCaptureConfiguration(AttributeSet attributes,
                                         ConnectionFactoryConfiguration cfc,
                                         TableConfiguration tc,
                                         Properties connectorProperties) {
      super(Element.CDC, attributes, null, cfc);
      this.attributes = attributes;
      this.tableConfiguration = tc;
      this.connectorProperties = connectorProperties;
   }

   static AttributeSet attributeSet() {
      return new AttributeSet(ChangeDataCaptureConfiguration.class,
            AbstractJdbcStoreConfiguration.attributeDefinitionSet(),
            ENABLED, FOREIGN_KEYS);
   }

   static AttributeSet protectAttributeSet(AttributeSet attributes) {
      attributes.attribute(SHARED).set(false);
      attributes.attribute(PRELOAD).set(false);
      attributes.attribute(PURGE_ON_STARTUP).set(false);
      attributes.attribute(TRANSACTIONAL).set(false);
      attributes.attribute(READ_ONLY).set(true);
      attributes.attribute(WRITE_ONLY).set(false);
      attributes.attribute(MAX_BATCH_SIZE).set(100);
      return attributes.protect();
   }

   public boolean enabled() {
      return attributes.attribute(ENABLED).get();
   }

   public Set<String> foreignKeys() {
      return attributes.attribute(FOREIGN_KEYS).get();
   }

   public TableConfiguration table() {
      return tableConfiguration;
   }

   public Properties connectorProperties() {
      return connectorProperties;
   }

   @Override
   public String toString() {
      return super.toString();
   }
}

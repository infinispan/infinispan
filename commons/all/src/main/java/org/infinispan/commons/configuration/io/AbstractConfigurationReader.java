package org.infinispan.commons.configuration.io;

import java.net.URL;
import java.util.Map;
import java.util.Properties;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 12.1
 **/
public abstract class AbstractConfigurationReader implements ConfigurationReader {
   private final String name;
   private final Properties properties;
   private final PropertyReplacer replacer;
   private final ConfigurationResourceResolver resolver;
   protected final NamingStrategy namingStrategy;
   private ConfigurationSchemaVersion schema;

   protected AbstractConfigurationReader(ConfigurationResourceResolver resolver, Properties properties, PropertyReplacer replacer, NamingStrategy namingStrategy) {
      URL context = resolver.getContext();
      this.name = context != null ? context.getPath() : null;
      this.resolver = resolver;
      this.properties = properties;
      this.replacer = replacer;
      this.namingStrategy = namingStrategy;
   }

   @Override
   public String getName() {
      return name;
   }

   @Override
   public ConfigurationResourceResolver getResourceResolver() {
      return resolver;
   }

   @Override
   public NamingStrategy getNamingStrategy() {
      return namingStrategy;
   }

   @Override
   public <T> T getProperty(String name) {
      return (T) properties.get(name);
   }

   @Override
   public Properties getProperties() {
      return properties;
   }

   @Override
   public ConfigurationSchemaVersion getSchema() {
      return schema;
   }

   @Override
   public void setSchema(ConfigurationSchemaVersion schema) {
      this.schema = schema;
   }

   @Override
   public void handleAny(ConfigurationReaderContext context) {
      require(ElementType.START_ELEMENT);
      context.handleAnyElement(this);
   }

   @Override
   public void handleAttribute(ConfigurationReaderContext context, int i) {
      require(ElementType.START_ELEMENT);
      context.handleAnyAttribute(this, i);
   }

   @Override
   public String getAttributeName(int index) {
      return getAttributeName(index, namingStrategy);
   }

   @Override
   public String getLocalName() {
      return getLocalName(namingStrategy);
   }

   @Override
   public String getAttributeValue(String name) {
      return getAttributeValue(name, namingStrategy);
   }

   @Override
   public Map.Entry<String, String> getMapItem(Enum<?> nameAttribute) {
      return getMapItem(nameAttribute.toString());
   }

   @Override
   public String[] readArray(Enum<?> outer, Enum<?> inner) {
      return readArray(outer.toString(), inner.toString());
   }

   protected String replaceProperties(String value) {
      return replacer.replaceProperties(value, properties);
   }
}

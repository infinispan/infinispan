package org.infinispan.server.configuration;

import java.util.ArrayList;
import java.util.List;

import org.infinispan.commons.configuration.ConfigurationInfo;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.elements.DefaultElementDefinition;
import org.infinispan.commons.configuration.elements.ElementDefinition;

/**
 * Holds configuration related to data sources
 * @author Tristan Tarrant
 * @since 11.0
 */
public class DataSourcesConfiguration implements ConfigurationInfo {

   private static final ElementDefinition ELEMENT_DEFINITION = new DefaultElementDefinition(Element.DATA_SOURCES.toString());

   private final List<DataSourceConfiguration> dataSources;
   private final List<ConfigurationInfo> configs = new ArrayList<>();

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(DataSourcesConfiguration.class);
   }

   private final AttributeSet attributes;

   DataSourcesConfiguration(AttributeSet attributes, List<DataSourceConfiguration> dataSources) {
      this.attributes = attributes.checkProtection();
      this.dataSources = dataSources;
      this.configs.addAll(dataSources);
   }

   @Override
   public List<ConfigurationInfo> subElements() {
      return configs;
   }

   @Override
   public AttributeSet attributes() {
      return attributes;
   }

   public List<DataSourceConfiguration> dataSources() {
      return dataSources;
   }

   @Override
   public ElementDefinition getElementDefinition() {
      return ELEMENT_DEFINITION;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      DataSourcesConfiguration that = (DataSourcesConfiguration) o;

      return attributes.equals(that.attributes);
   }

   @Override
   public int hashCode() {
      return attributes.hashCode();
   }

   @Override
   public String toString() {
      return "DataSourcesConfiguration{" +
            "attributes=" + attributes +
            '}';
   }
}

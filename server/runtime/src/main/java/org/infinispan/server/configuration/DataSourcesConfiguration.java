package org.infinispan.server.configuration;

import java.util.List;

import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.ConfigurationElement;

/**
 * Holds configuration related to data sources
 * @author Tristan Tarrant
 * @since 11.0
 */
public class DataSourcesConfiguration extends ConfigurationElement<DataSourcesConfiguration> {

   private final List<DataSourceConfiguration> dataSources;

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(DataSourcesConfiguration.class);
   }

   DataSourcesConfiguration(AttributeSet attributes, List<DataSourceConfiguration> dataSources) {
      super(Element.DATA_SOURCES, attributes);
      this.dataSources = dataSources;
   }

   public List<DataSourceConfiguration> dataSources() {
      return dataSources;
   }
}

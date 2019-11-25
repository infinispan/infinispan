package org.infinispan.server.configuration;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.attributes.AttributeSet;

public class DataSourcesConfigurationBuilder implements Builder<DataSourcesConfiguration> {

   private final AttributeSet attributes;
   private final ServerConfigurationBuilder server;

   private Map<String, DataSourceConfigurationBuilder> dataSources = new LinkedHashMap<>(2);

   DataSourcesConfigurationBuilder(ServerConfigurationBuilder server) {
      this.server = server;
      attributes = DataSourcesConfiguration.attributeDefinitionSet();
   }

   DataSourceConfigurationBuilder dataSource(String name, String jndiName) {
      DataSourceConfigurationBuilder builder = new DataSourceConfigurationBuilder(server, name, jndiName);
      dataSources.put(name, builder);
      return builder;
   }

   @Override
   public void validate() {
   }

   @Override
   public DataSourcesConfiguration create() {
      List<DataSourceConfiguration> list = dataSources.values().stream()
            .map(DataSourceConfigurationBuilder::create).collect(Collectors.toList());
      return new DataSourcesConfiguration(attributes.protect(), list);
   }

   @Override
   public DataSourcesConfigurationBuilder read(DataSourcesConfiguration template) {
      this.attributes.read(template.attributes());
      dataSources.clear();
      //template.dataSources().forEach(s -> dataSource(s.name(), s.port(), s.interfaceName()));
      return this;
   }
}

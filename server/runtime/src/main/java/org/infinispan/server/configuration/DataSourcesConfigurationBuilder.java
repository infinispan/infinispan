package org.infinispan.server.configuration;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.server.Server;

public class DataSourcesConfigurationBuilder implements Builder<DataSourcesConfiguration> {

   private final AttributeSet attributes;
   private final Map<String, DataSourceConfigurationBuilder> dataSources = new LinkedHashMap<>(2);
   private final Set<String> jndiNames = new HashSet<>(2);

   DataSourcesConfigurationBuilder() {
      attributes = DataSourcesConfiguration.attributeDefinitionSet();
   }

   DataSourceConfigurationBuilder dataSource(String name, String jndiName) {
      if (dataSources.containsKey(name)) {
         throw Server.log.duplicateDataSource(name);
      }
      if (jndiNames.contains(jndiName)) {
         throw Server.log.duplicateJndiName(jndiName);
      }
      DataSourceConfigurationBuilder builder = new DataSourceConfigurationBuilder(name, jndiName);
      dataSources.put(name, builder);
      jndiNames.add(jndiName);
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

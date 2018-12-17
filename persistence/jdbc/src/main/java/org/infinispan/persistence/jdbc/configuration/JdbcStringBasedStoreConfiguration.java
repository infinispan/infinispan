package org.infinispan.persistence.jdbc.configuration;

import static org.infinispan.persistence.jdbc.configuration.Element.STRING_KEYED_JDBC_STORE;

import java.util.ArrayList;
import java.util.List;

import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.commons.configuration.ConfigurationFor;
import org.infinispan.commons.configuration.ConfigurationInfo;
import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.elements.DefaultElementDefinition;
import org.infinispan.commons.configuration.elements.ElementDefinition;
import org.infinispan.configuration.cache.AsyncStoreConfiguration;
import org.infinispan.configuration.cache.SingletonStoreConfiguration;
import org.infinispan.configuration.serializing.SerializedWith;
import org.infinispan.persistence.jdbc.stringbased.JdbcStringBasedStore;
import org.infinispan.persistence.keymappers.DefaultTwoWayKey2StringMapper;

@BuiltBy(JdbcStringBasedStoreConfigurationBuilder.class)
@ConfigurationFor(JdbcStringBasedStore.class)
@SerializedWith(JdbcStringBasedStoreConfigurationSerializer.class)
public class JdbcStringBasedStoreConfiguration extends AbstractJdbcStoreConfiguration implements ConfigurationInfo {
   static final AttributeDefinition<String> KEY2STRING_MAPPER = AttributeDefinition.builder("key2StringMapper" , DefaultTwoWayKey2StringMapper.class.getName()).immutable().xmlName("key-to-string-mapper").build();
   private final List<ConfigurationInfo> subElements;

   public static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(JdbcStringBasedStoreConfiguration.class, AbstractJdbcStoreConfiguration.attributeDefinitionSet(), KEY2STRING_MAPPER);
   }

   static ElementDefinition ELEMENT_DEFINITION = new DefaultElementDefinition(STRING_KEYED_JDBC_STORE.getLocalName());

   private final Attribute<String> key2StringMapper;
   private final TableManipulationConfiguration table;

   public JdbcStringBasedStoreConfiguration(AttributeSet attributes, AsyncStoreConfiguration async, SingletonStoreConfiguration singletonStore,
                                            ConnectionFactoryConfiguration connectionFactory, TableManipulationConfiguration table) {
      super(attributes, async, singletonStore, connectionFactory);
      this.table = table;
      key2StringMapper = attributes.attribute(KEY2STRING_MAPPER);
      subElements = new ArrayList<>(super.subElements());
      subElements.add(table);
   }

   @Override
   public ElementDefinition getElementDefinition() {
      return ELEMENT_DEFINITION;
   }

   @Override
   public List<ConfigurationInfo> subElements() {
      return subElements;
   }

   public String key2StringMapper() {
      return key2StringMapper.get();
   }

   public TableManipulationConfiguration table() {
      return table;
   }

   @Override
   public String toString() {
      return "JdbcStringBasedStoreConfiguration [table=" + table + ", attributes=" + attributes + ", connectionFactory=" + connectionFactory() + ", async=" + async()
            + ", singletonStore()=" + singletonStore() + "]";
   }

   @Override
   public int hashCode() {
      final int prime = 31;
      int result = super.hashCode();
      result = prime * result + ((table == null) ? 0 : table.hashCode());
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
      JdbcStringBasedStoreConfiguration other = (JdbcStringBasedStoreConfiguration) obj;
      if (table == null) {
         if (other.table != null)
            return false;
      } else if (!table.equals(other.table))
         return false;
      return true;
   }
}

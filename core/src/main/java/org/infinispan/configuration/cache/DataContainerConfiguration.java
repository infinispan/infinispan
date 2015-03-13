package org.infinispan.configuration.cache;

import org.infinispan.commons.configuration.AbstractTypedPropertiesConfiguration;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.equivalence.AnyEquivalence;
import org.infinispan.commons.equivalence.Equivalence;
import org.infinispan.container.DataContainer;

/**
 * Controls the data container for the cache.
 *
 * @author pmuir
 *
 */
public class DataContainerConfiguration extends AbstractTypedPropertiesConfiguration {
   static final AttributeDefinition<DataContainer> DATA_CONTAINER = AttributeDefinition
         .builder("dataContainer", null, DataContainer.class).immutable().build();
   static final AttributeDefinition<Equivalence> KEY_EQUIVALENCE = AttributeDefinition
         .<Equivalence> builder("keyEquivalence", AnyEquivalence.getInstance()).immutable().build();
   static final AttributeDefinition<Equivalence> VALUE_EQUIVALENCE = AttributeDefinition
         .<Equivalence> builder("valueEquivalence", AnyEquivalence.getInstance()).immutable().build();

   static public AttributeSet attributeSet() {
      return new AttributeSet(DataContainerConfiguration.class, AbstractTypedPropertiesConfiguration.attributeSet(),
            DATA_CONTAINER, KEY_EQUIVALENCE, VALUE_EQUIVALENCE);
   }

   DataContainerConfiguration(AttributeSet attributes) {
      super(attributes);
   }

   /**
    * Data container implementation in use
    *
    * @return
    */
   public DataContainer dataContainer() {
      return attributes.attribute(DATA_CONTAINER).asObject(DataContainer.class);
   }

   @SuppressWarnings("unchecked")
   public <K> Equivalence<K> keyEquivalence() {
      return (Equivalence<K>) attributes.attribute(KEY_EQUIVALENCE).asObject(DataContainer.class);
   }

   @SuppressWarnings("unchecked")
   public <V> Equivalence<V> valueEquivalence() {
      return (Equivalence<V>) attributes.attribute(VALUE_EQUIVALENCE).asObject(DataContainer.class);
   }

   AttributeSet attributes() {
      return attributes;
   }

   @Override
   public String toString() {
      return "DataContainerConfiguration [attributes=" + attributes + "]";
   }

}

package org.infinispan.configuration.cache;

import org.infinispan.commons.configuration.AbstractTypedPropertiesConfiguration;
import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.IdentityAttributeCopier;
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
   public static final AttributeDefinition<DataContainer> DATA_CONTAINER = AttributeDefinition
         .builder("dataContainer", null, DataContainer.class).copier(IdentityAttributeCopier.INSTANCE).immutable().build();
   public static final AttributeDefinition<Equivalence> KEY_EQUIVALENCE = AttributeDefinition
         .<Equivalence> builder("keyEquivalence", AnyEquivalence.getInstance()).copier(IdentityAttributeCopier.INSTANCE).immutable().build();
   public static final AttributeDefinition<Equivalence> VALUE_EQUIVALENCE = AttributeDefinition
         .<Equivalence> builder("valueEquivalence", AnyEquivalence.getInstance()).copier(IdentityAttributeCopier.INSTANCE).immutable().build();

   static public AttributeSet attributeDefinitionSet() {
      return new AttributeSet(DataContainerConfiguration.class, AbstractTypedPropertiesConfiguration.attributeSet(),
            DATA_CONTAINER, KEY_EQUIVALENCE, VALUE_EQUIVALENCE);
   }

   private final Attribute<DataContainer> dataContainer;
   private final Attribute<Equivalence> keyEquivalence;
   private final Attribute<Equivalence> valueEquivalence;

   DataContainerConfiguration(AttributeSet attributes) {
      super(attributes);
      dataContainer = attributes.attribute(DATA_CONTAINER);
      keyEquivalence = attributes.attribute(KEY_EQUIVALENCE);
      valueEquivalence = attributes.attribute(VALUE_EQUIVALENCE);
   }

   /**
    * Data container implementation in use
    *
    * @return
    */
   public DataContainer dataContainer() {
      return dataContainer.get();
   }

   @SuppressWarnings("unchecked")
   public <K> Equivalence<K> keyEquivalence() {
      return keyEquivalence.get();
   }

   @SuppressWarnings("unchecked")
   public <V> Equivalence<V> valueEquivalence() {
      return valueEquivalence.get();
   }

   public AttributeSet attributes() {
      return attributes;
   }

   @Override
   public String toString() {
      return "DataContainerConfiguration [attributes=" + attributes + "]";
   }

}

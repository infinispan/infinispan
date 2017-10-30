package org.infinispan.configuration.cache;

import org.infinispan.commons.configuration.AbstractTypedPropertiesConfiguration;
import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.IdentityAttributeCopier;
import org.infinispan.commons.configuration.attributes.Matchable;
import org.infinispan.commons.equivalence.AnyEquivalence;
import org.infinispan.commons.equivalence.Equivalence;
import org.infinispan.container.DataContainer;

/**
 * Controls the data container for the cache.
 *
 * @author pmuir
 * @deprecated Please use {@link MemoryConfiguration}
 */
@Deprecated
public class DataContainerConfiguration extends AbstractTypedPropertiesConfiguration implements Matchable<DataContainerConfiguration> {
   public static final AttributeDefinition<DataContainer> DATA_CONTAINER = AttributeDefinition
         .builder("dataContainer", null, DataContainer.class).xmlName("class").copier(IdentityAttributeCopier.INSTANCE).immutable().build();

   static public AttributeSet attributeDefinitionSet() {
      return new AttributeSet(DataContainerConfiguration.class, AbstractTypedPropertiesConfiguration.attributeSet(),
            DATA_CONTAINER);
   }

   private final Attribute<DataContainer> dataContainer;

   DataContainerConfiguration(AttributeSet attributes) {
      super(attributes);
      dataContainer = attributes.attribute(DATA_CONTAINER);
   }

   /**
    * Data container implementation in use
    *
    * @return
    * @deprecated data container is no longer to exposed via configuration at a later point
    */
   @Deprecated
   public DataContainer dataContainer() {
      return dataContainer.get();
   }

   /**
    * @deprecated Equivalence is no longer used.  This will be removed in the future.  Only returns {@link AnyEquivalence}
    */
   @Deprecated
   public <K> Equivalence<K> keyEquivalence() {
      return AnyEquivalence.getInstance();
   }

   /**
    * @deprecated Equivalence is no longer used.  This will be removed in the future.  Only returns {@link AnyEquivalence}
    */
   @Deprecated
   public <V> Equivalence<V> valueEquivalence() {
      return AnyEquivalence.getInstance();
   }

   public AttributeSet attributes() {
      return attributes;
   }

   @Override
   public String toString() {
      return "DataContainerConfiguration [attributes=" + attributes + "]";
   }

}

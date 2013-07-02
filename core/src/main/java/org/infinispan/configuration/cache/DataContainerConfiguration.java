package org.infinispan.configuration.cache;

import org.infinispan.commons.configuration.AbstractTypedPropertiesConfiguration;
import org.infinispan.commons.equivalence.Equivalence;
import org.infinispan.commons.util.TypedProperties;
import org.infinispan.container.DataContainer;

/**
 * Controls the data container for the cache.
 * 
 * @author pmuir
 *
 */
public class DataContainerConfiguration extends AbstractTypedPropertiesConfiguration {

   private final DataContainer dataContainer;
   private final Equivalence keyEquivalence;
   private final Equivalence valueEquivalence;

   DataContainerConfiguration(DataContainer dataContainer,
         TypedProperties properties, Equivalence keyEquivalence,
         Equivalence valueEquivalence) {
      super(properties);
      this.dataContainer = dataContainer;
      this.keyEquivalence = keyEquivalence;
      this.valueEquivalence = valueEquivalence;
   }
   
   /**
    * Data container implementation in use
    * @return
    */
   public DataContainer dataContainer() {
      return dataContainer;
   }

   @SuppressWarnings("unchecked")
   public <K> Equivalence<K> keyEquivalence() {
      return keyEquivalence;
   }

   @SuppressWarnings("unchecked")
   public <V> Equivalence<V> valueEquivalence() {
      return valueEquivalence;
   }

   @Override
   public String toString() {
      return "DataContainerConfiguration{" +
            "dataContainer=" + dataContainer +
            ", keyEquivalence=" + keyEquivalence +
            ", valueEquivalence=" + valueEquivalence +
            '}';
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      if (!super.equals(o)) return false;

      DataContainerConfiguration that = (DataContainerConfiguration) o;

      if (dataContainer != null ? !dataContainer.equals(that.dataContainer) : that.dataContainer != null)
         return false;
      if (keyEquivalence != null ? !keyEquivalence.equals(that.keyEquivalence) : that.keyEquivalence != null)
         return false;
      if (valueEquivalence != null ? !valueEquivalence.equals(that.valueEquivalence) : that.valueEquivalence != null)
         return false;

      return true;
   }

   @Override
   public int hashCode() {
      int result = super.hashCode();
      result = 31 * result + (dataContainer != null ? dataContainer.hashCode() : 0);
      result = 31 * result + (keyEquivalence != null ? keyEquivalence.hashCode() : 0);
      result = 31 * result + (valueEquivalence != null ? valueEquivalence.hashCode() : 0);
      return result;
   }

}

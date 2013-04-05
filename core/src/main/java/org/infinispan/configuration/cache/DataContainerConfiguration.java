/*
 * Copyright 2011 Red Hat, Inc. and/or its affiliates.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA
 */
package org.infinispan.configuration.cache;

import org.infinispan.configuration.AbstractTypedPropertiesConfiguration;
import org.infinispan.container.DataContainer;
import org.infinispan.util.AnyEquivalence;
import org.infinispan.util.Equivalence;
import org.infinispan.util.TypedProperties;

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
         TypedProperties properties, Equivalence keyEquivalence, Equivalence valueEquivalence) {
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

   public Equivalence keyEquivalence() {
      return keyEquivalence;
   }

   public Equivalence valueEquivalence() {
      return valueEquivalence;
   }

   public boolean isEquivalentContainer() {
      return keyEquivalence != AnyEquivalence.OBJECT
            || valueEquivalence != AnyEquivalence.OBJECT;
   }

   @Override
   public String toString() {
      return "DataContainerConfiguration{" +
            "dataContainer=" + dataContainer +
            ", comparingKey=" + keyEquivalence +
            ", comparingValue=" + valueEquivalence +
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

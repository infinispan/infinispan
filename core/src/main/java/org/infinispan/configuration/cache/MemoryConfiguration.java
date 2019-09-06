package org.infinispan.configuration.cache;

import static org.infinispan.configuration.parsing.Element.MEMORY;

import java.util.Collections;
import java.util.List;

import org.infinispan.commons.configuration.ConfigurationInfo;
import org.infinispan.commons.configuration.attributes.Matchable;
import org.infinispan.commons.configuration.elements.DefaultElementDefinition;
import org.infinispan.commons.configuration.elements.ElementDefinition;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.eviction.EvictionType;

/**
 * Controls the memory storage configuration for the cache.
 *
 * @author William Burns
 */
public class MemoryConfiguration implements Matchable<MemoryConfiguration>, ConfigurationInfo {

   private final List<ConfigurationInfo> subElements;
   private final MemoryStorageConfiguration memoryStorageConfiguration;

   public static final ElementDefinition ELEMENT_DEFINITION = new DefaultElementDefinition(MEMORY.getLocalName());

   MemoryConfiguration(MemoryStorageConfiguration memoryStorageConfiguration) {
      this.memoryStorageConfiguration = memoryStorageConfiguration;
      this.subElements = Collections.singletonList(memoryStorageConfiguration);
   }

   @Override
   public ElementDefinition getElementDefinition() {
      return ELEMENT_DEFINITION;
   }

   @Override
   public List<ConfigurationInfo> subElements() {
      return subElements;
   }

   /**
    * Storage type to use for the data container
    * @return
    */
   public StorageType storageType() {
      return memoryStorageConfiguration.storageType();
   }

   /**
    * Size of the eviction, -1 if disabled
    * @return
    */
   public long size() {
      return memoryStorageConfiguration.size();
   }

   public void size(long newSize) {
      memoryStorageConfiguration.size(newSize);
   }

   /**
    * The configured eviction type
    * @return
    */
   public EvictionType evictionType() {
      return memoryStorageConfiguration.evictionType();
   }

   /**
    * The configured eviction strategy
    * @return
    */
   public EvictionStrategy evictionStrategy() {
      return memoryStorageConfiguration.evictionStrategy();
   }

   /**
    * Returns whether remove eviction is in use
    * @return
    */
   public boolean isEvictionEnabled() {
      return memoryStorageConfiguration.size() > 0 && memoryStorageConfiguration.evictionStrategy().isRemovalBased();
   }

   /**
    * The address pointer count
    * @return
    * @deprecated since 10.0
    */
   @Deprecated
   public int addressCount() {
      return memoryStorageConfiguration.addressCount();
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      MemoryConfiguration that = (MemoryConfiguration) o;

      return memoryStorageConfiguration.equals(that.memoryStorageConfiguration);
   }

   @Override
   public int hashCode() {
      return memoryStorageConfiguration.hashCode();
   }

   @Override
   public String toString() {
      return "MemoryConfiguration{" +
            "memoryStorageConfiguration=" + memoryStorageConfiguration +
            '}';
   }

   public MemoryStorageConfiguration heapConfiguration() {
      return memoryStorageConfiguration;
   }
}

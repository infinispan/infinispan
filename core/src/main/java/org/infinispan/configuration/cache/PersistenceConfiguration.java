package org.infinispan.configuration.cache;

import java.util.List;

import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.ConfigurationElement;
import org.infinispan.configuration.parsing.Attribute;
import org.infinispan.configuration.parsing.Element;

/**
 * Configuration for stores.
 */
public class PersistenceConfiguration extends ConfigurationElement<PersistenceConfiguration> {
   public static final AttributeDefinition<Boolean> PASSIVATION = AttributeDefinition.builder(org.infinispan.configuration.parsing.Attribute.PASSIVATION, false).immutable().build();
   public static final AttributeDefinition<Integer> AVAILABILITY_INTERVAL = AttributeDefinition.builder(org.infinispan.configuration.parsing.Attribute.AVAILABILITY_INTERVAL, 30000).immutable().build();
   public static final AttributeDefinition<Integer> CONNECTION_ATTEMPTS = AttributeDefinition.builder(org.infinispan.configuration.parsing.Attribute.CONNECTION_ATTEMPTS, 10).build();
   @Deprecated
   public static final AttributeDefinition<Integer> CONNECTION_INTERVAL = AttributeDefinition.builder(Attribute.CONNECTION_INTERVAL, 50).immutable().deprecatedSince(15, 0).build();

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(PersistenceConfiguration.class, PASSIVATION, AVAILABILITY_INTERVAL, CONNECTION_ATTEMPTS, CONNECTION_INTERVAL);
   }

   private final List<StoreConfiguration> stores;


   PersistenceConfiguration(AttributeSet attributes, List<StoreConfiguration> stores) {
      super(Element.PERSISTENCE, attributes, asChildren(stores));
      this.stores = stores;
   }

   private static ConfigurationElement<?>[] asChildren(List<StoreConfiguration> stores) {
      return stores.stream().filter(store -> store instanceof ConfigurationElement).toArray(ConfigurationElement[]::new);
   }

   /**
    * If true, data is only written to the cache store when it is evicted from memory, a phenomenon known as
    * 'passivation'. Next time the data is requested, it will be 'activated' which means that data will be brought back
    * to memory and removed from the persistent store. This gives you the ability to 'overflow' to disk, similar to
    * swapping in an operating system. <br /> <br /> If false, the cache store contains a copy of the contents in
    * memory, so writes to cache result in cache store writes. This essentially gives you a 'write-through'
    * configuration.
    */
   public boolean passivation() {
      return attributes.attribute(PASSIVATION).get();
   }

   public int availabilityInterval() {
      return attributes.attribute(AVAILABILITY_INTERVAL).get();
   }

   public int connectionAttempts() {
      return attributes.attribute(CONNECTION_ATTEMPTS).get();
   }

   @Deprecated
   public int connectionInterval() {
      return -1;
   }

   public List<StoreConfiguration> stores() {
      return stores;
   }

   /**
    * Loops through all individual cache loader configs and checks if fetchPersistentState is set on any of them
    *
    * @deprecated since 14.0. This will always return false
    */
   @Deprecated
   public Boolean fetchPersistentState() {
      return false;
   }

   /**
    * Loops through all individual cache loader configs and checks if preload is set on any of them
    */
   public Boolean preload() {
      for (StoreConfiguration c : stores) {
         if (c.preload())
            return true;
      }
      return false;
   }

   public boolean usingStores() {
      return !stores.isEmpty();
   }

   public boolean usingAsyncStore() {
      for (StoreConfiguration c : stores) {
         if (c.async().enabled())
            return true;
      }
      return false;
   }

   /**
    * Returns if any store is {@link StoreConfiguration#segmented()}
    *
    * @return true if any configured store is segmented, otherwise false
    */
   public boolean usingSegmentedStore() {
      for (StoreConfiguration c : stores) {
         if (c.segmented())
            return true;
      }
      return false;
   }
}

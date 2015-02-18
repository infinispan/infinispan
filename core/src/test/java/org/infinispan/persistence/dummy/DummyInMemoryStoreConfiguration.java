package org.infinispan.persistence.dummy;

import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.commons.configuration.ConfigurationFor;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.configuration.cache.AbstractStoreConfiguration;
import org.infinispan.configuration.cache.AsyncStoreConfiguration;
import org.infinispan.configuration.cache.SingletonStoreConfiguration;

@BuiltBy(DummyInMemoryStoreConfigurationBuilder.class)
@ConfigurationFor(DummyInMemoryStore.class)
public class DummyInMemoryStoreConfiguration extends AbstractStoreConfiguration {
   static final AttributeDefinition<Boolean> DEBUG = AttributeDefinition.builder("debug", false).immutable().build();
   static final AttributeDefinition<Boolean> SLOW = AttributeDefinition.builder("slow", false).immutable().build();
   static final AttributeDefinition<String> STORE_NAME = AttributeDefinition.builder("storeName", null, String.class).immutable().build();
   static final AttributeDefinition<Object> FAIL_KEY = AttributeDefinition.builder("failKey", null, Object.class).immutable().build();
   public static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(DummyInMemoryStore.class, AbstractStoreConfiguration.attributeDefinitionSet(), DEBUG, SLOW, STORE_NAME, FAIL_KEY);
   }

   public DummyInMemoryStoreConfiguration(AttributeSet attributes, AsyncStoreConfiguration async, SingletonStoreConfiguration singletonStore) {
      super(attributes, async, singletonStore);
   }

   public boolean debug() {
      return attributes.attribute(DEBUG).get();
   }

   public boolean slow() {
      return attributes.attribute(SLOW).get();
   }

   public String storeName() {
      return attributes.attribute(STORE_NAME).get();
   }

   public Object failKey() {
      return attributes.attribute(FAIL_KEY).get();
   }


}

package org.infinispan.persistence.dummy;

import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.commons.configuration.ConfigurationFor;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.configuration.cache.AbstractStoreConfiguration;
import org.infinispan.configuration.cache.AsyncStoreConfiguration;

@BuiltBy(DummyInMemoryStoreConfigurationBuilder.class)
@ConfigurationFor(DummyInMemoryStore.class)
public class DummyInMemoryStoreConfiguration extends AbstractStoreConfiguration<DummyInMemoryStoreConfiguration> {
   static final AttributeDefinition<Boolean> SLOW = AttributeDefinition.builder("slow", false).immutable().build();
   static final AttributeDefinition<Boolean> ASYNC_OPERATION = AttributeDefinition.builder("async-operation", false).immutable().build();
   static final AttributeDefinition<String> STORE_NAME = AttributeDefinition.builder("store-name", null, String.class).immutable().build();
   static final AttributeDefinition<Integer> START_FAILURES = AttributeDefinition.builder("start-failures", 0).immutable().build();
   public static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(DummyInMemoryStoreConfiguration.class, AbstractStoreConfiguration.attributeDefinitionSet(), SLOW, ASYNC_OPERATION, STORE_NAME, START_FAILURES);
   }

   public DummyInMemoryStoreConfiguration(AttributeSet attributes, AsyncStoreConfiguration async) {
      super(Element.DUMMY_STORE, attributes, async);
   }

   public boolean slow() {
      return attributes.attribute(SLOW).get();
   }

   public boolean asyncOperation() {
      return attributes.attribute(ASYNC_OPERATION).get();
   }

   public String storeName() {
      return attributes.attribute(STORE_NAME).get();
   }

   public int startFailures() {
      return attributes.attribute(START_FAILURES).get();
   }
}

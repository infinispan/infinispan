package org.infinispan.counter.configuration;

import static org.infinispan.counter.logging.Log.CONTAINER;

import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.counter.api.Storage;

/**
 * Base counter configuration with its name, initial value and {@link Storage} mode.
 *
 * @author Pedro Ruivo
 * @since 9.0
 */
public abstract class AbstractCounterConfiguration {

   static final AttributeDefinition<Long> INITIAL_VALUE = AttributeDefinition.builder(Attribute.INITIAL_VALUE, 0L)
         .immutable()
         .build();
   static final AttributeDefinition<Storage> STORAGE = AttributeDefinition.builder(Attribute.STORAGE, Storage.VOLATILE)
         .validator(value -> {
            if (value == null) {
               throw CONTAINER.invalidStorageMode();
            }
         })
         .immutable()
         .build();
   static final AttributeDefinition<String> NAME = AttributeDefinition.builder(Attribute.NAME, null, String.class)
         .validator(value -> {
            if (value == null) {
               throw CONTAINER.missingCounterName();
            }
         }).autoPersist(false)
         .immutable()
         .build();
   final AttributeSet attributes;

   AbstractCounterConfiguration(AttributeSet attributes) {
      this.attributes = attributes;
   }

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(AbstractCounterConfiguration.class, NAME, INITIAL_VALUE, STORAGE);
   }

   public final AttributeSet attributes() {
      return attributes;
   }

   public String name() {
      return attributes.attribute(NAME).get();
   }

   public long initialValue() {
      return attributes.attribute(INITIAL_VALUE).get();
   }

   public Storage storage() {
      return attributes.attribute(STORAGE).get();
   }

}

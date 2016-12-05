package org.infinispan.counter.configuration;

import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.counter.api.Storage;
import org.infinispan.counter.logging.Log;

/**
 * Base counter configuration with its name, initial value and {@link Storage} mode.
 *
 * @author Pedro Ruivo
 * @since 9.0
 */
public abstract class AbstractCounterConfiguration {

   static final AttributeDefinition<Long> INITIAL_VALUE = AttributeDefinition.builder("initialValue", 0L)
         .xmlName("initial-value")
         .immutable()
         .build();
   private static final Log log = LogFactory.getLog(AbstractCounterConfiguration.class, Log.class);
   static final AttributeDefinition<Storage> STORAGE = AttributeDefinition.builder("storage", Storage.VOLATILE)
         .xmlName("storage")
         .validator(value -> {
            if (value == null) {
               throw log.invalidStorageMode();
            }
         })
         .immutable()
         .build();
   static final AttributeDefinition<String> NAME = AttributeDefinition.builder("name", null, String.class)
         .xmlName("name")
         .validator(value -> {
            if (value == null) {
               throw log.missingCounterName();
            }
         })
         .immutable()
         .build();
   final AttributeSet attributes;

   AbstractCounterConfiguration(AttributeSet attributes) {
      this.attributes = attributes;
   }

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(AbstractCounterConfiguration.class, NAME, INITIAL_VALUE, STORAGE);
   }

   final AttributeSet attributes() {
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

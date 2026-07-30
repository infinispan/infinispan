package org.infinispan.globalstate.impl;

import java.util.function.BiConsumer;
import java.util.function.Predicate;

import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.ConfigurationElement;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.globalstate.ScopedPersistentState;

final class ConfigurationUtil {

   private ConfigurationUtil() { }

   /**
    * Restores per-node mutable attribute values from previously persisted state.
    *
    * <p>
    * Recursively walks the configuration tree and sets each matching attribute to the value found in the scoped state.
    * Attributes absent from the state are left unchanged.
    * </p>
    *
    * @param configuration the configuration to apply values to
    * @param state   the persisted per-node state to read from
    */
   public static void applyLocalAttributes(Configuration configuration, ScopedPersistentState state) {
      forEachAttribute(configuration, ConfigurationUtil::isLocalMutable, (attr, def) -> {
         String value = state.getProperty(def.name());
         if (value != null) {
            attr.set(def.parse(value));
         }
      });
   }

   /**
    * Persists per-node mutable attribute values into the given scoped state.
    *
    * <p>
    * Recursively walks the configuration tree and writes each matching attribute as a string property keyed by its
    * definition name.
    * </p>
    *
    * @param configuration the configuration to read values from
    * @param state   the scoped state to write into
    */
   public static void collectLocalAttributesInto(Configuration configuration, ScopedPersistentState state) {
      forEachAttribute(configuration, ConfigurationUtil::isLocalMutable, (attr, def) -> {
         if (attr.isModified()) {
            state.setProperty(def.name(), String.valueOf(attr.get()));
         }
      });
   }

   /**
    * Resets per-node mutable attributes to their definition defaults.
    *
    * <p>
    * Used to produce a sanitised copy of the configuration before serialising into shared storage. The provided configuration
    * is modified in place, where the mutable local attributes reset to the default values.
    * </p>
    *
    * @param configuration the configuration to reset
    */
   public static void resetLocalMutableAttributes(Configuration configuration) {
      forEachAttribute(configuration, ConfigurationUtil::isLocalMutable,
            (attr, definition) -> attr.reset());
   }

   public static void resetGlobalMutableAttributes(Configuration configuration) {
      forEachAttribute(configuration, ConfigurationUtil::isGlobalMutable,
            (attr, def) -> attr.reset());
   }

   private static void forEachAttribute(ConfigurationElement<?> element,
                                        Predicate<AttributeDefinition<?>> predicate,
                                        BiConsumer<Attribute<Object>, AttributeDefinition<Object>> consumer) {
      for (Attribute<?> attr : element.attributes().attributes()) {
         AttributeDefinition<?> definition = attr.getAttributeDefinition();
         if (predicate.test(definition)) {
            @SuppressWarnings("unchecked")
            AttributeDefinition<Object> uncheckedDef = (AttributeDefinition<Object>) definition;
            @SuppressWarnings("unchecked")
            Attribute<Object> uncheckedAttr = (Attribute<Object>) attr;
            consumer.accept(uncheckedAttr, uncheckedDef);
         }
      }

      for (ConfigurationElement<?> child : element.children()) {
         forEachAttribute(child, predicate, consumer);
      }
   }

   private static boolean isLocalMutable(AttributeDefinition<?> def) {
      return !def.isGlobal() && !def.isImmutable();
   }

   private static boolean isGlobalMutable(AttributeDefinition<?> def) {
      return def.isGlobal() && !def.isImmutable();
   }
}

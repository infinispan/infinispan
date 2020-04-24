package org.infinispan.configuration.cache;

import java.util.LinkedHashSet;
import java.util.Set;

import org.infinispan.commons.configuration.attributes.AttributeSet;

/**
 * Keeps track of modifications in a {@link AttributeSet}.
 *
 * @since 11.0
 */
class AttributeChangeTracker {
   private boolean isTracking = true;
   private final Set<String> changedAttributes = new LinkedHashSet<>();

   AttributeChangeTracker(AttributeSet attributeSet) {
      attributeSet.attributes().forEach(a -> a.addListener((attr, oldVal) -> {
         if (this.isTracking) {
            String name = attr.name();
            Object value = attr.get();
            changedAttributes.add(name + "=" + (value == null ? "" : value.toString()));
         }
      }));
   }

   /**
    * Stop tracking changes.
    */
   void stopChangeTracking() {
      this.isTracking = false;
   }

   boolean hasChanges() {
      return !changedAttributes.isEmpty();
   }

   /**
    * Resets the tracker to the initial state.
    */
   void reset() {
      changedAttributes.clear();
      isTracking = true;
   }

   /**
    * @return Comma separated list of changed attributes names and values.
    */
   String getChangedAttributes() {
      return String.join(", ", changedAttributes);
   }
}

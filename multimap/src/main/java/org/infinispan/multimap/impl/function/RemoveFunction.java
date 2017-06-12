package org.infinispan.multimap.impl.function;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.functional.EntryView;
import org.infinispan.multimap.impl.ExternalizerIds;

/**
 * Serializable function used by {@link org.infinispan.multimap.impl.EmbeddedMultimapCache#remove(Object)} and
 * {@link org.infinispan.multimap.impl.EmbeddedMultimapCache#remove(Object, Object)} to remove a key or a key/value
 * pair from the Multimap Cache, if such exists.
 * <p>
 * {@link #apply(EntryView.ReadWriteEntryView)} will return {@link Boolean#TRUE} when the operation removed a key or
 * a key/value pair and will return {@link Boolean#FALSE} if the key or key/value pair does not exist
 *
 * @author Katia Aresti - karesti@redhat.com
 * @see <a href="http://infinispan.org/documentation/">Marshalling of Functions</a>
 * @since 9.2
 */
public final class RemoveFunction<K, V> implements BaseFunction<K, V, Boolean> {

   public static final AdvancedExternalizer<RemoveFunction> EXTERNALIZER = new Externalizer();
   private final V value;

   /**
    * Call this constructor to create a function that removed a key
    */
   public RemoveFunction() {
      this.value = null;
   }

   /**
    * Call this constructor to create a function that removed a key/value pair
    *
    * @param value value to be removed
    */
   public RemoveFunction(V value) {
      this.value = value;
   }

   @Override
   public Boolean apply(EntryView.ReadWriteEntryView<K, Collection<V>> entryView) {
      Boolean removed;
      if (value == null) {
         removed = removeKey(entryView);
      } else {
         removed = removeKeyValue(entryView);
      }
      return removed;
   }

   private Boolean removeKeyValue(EntryView.ReadWriteEntryView<K, Collection<V>> entryView) {
      return entryView.find().map(values -> {
               if (values.contains(value)) {
                  Collection<V> newValues = new HashSet<>();
                  newValues.addAll(values);
                  newValues.remove(value);
                  if (newValues.isEmpty()) {
                     // If the collection is empty after remove, remove the key
                     entryView.remove();
                  } else {
                     entryView.set(newValues);
                  }
                  return newValues.size() < values.size();
               } else {
                  return Boolean.FALSE;
               }
            }
      ).orElse(Boolean.FALSE);
   }

   private Boolean removeKey(EntryView.ReadWriteEntryView<K, Collection<V>> entryView) {
      return entryView.find().map(values -> {
         entryView.remove();
         return Boolean.TRUE;
      }).orElse(Boolean.FALSE);
   }

   private static class Externalizer implements AdvancedExternalizer<RemoveFunction> {

      @Override
      public Set<Class<? extends RemoveFunction>> getTypeClasses() {
         return Collections.singleton(RemoveFunction.class);
      }

      @Override
      public Integer getId() {
         return ExternalizerIds.REMOVE_KEY_VALUE_FUNCTION;
      }

      @Override
      public void writeObject(ObjectOutput output, RemoveFunction object) throws IOException {
         output.writeObject(object.value);
      }

      @Override
      public RemoveFunction readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return new RemoveFunction(input.readObject());
      }
   }
}

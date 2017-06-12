package org.infinispan.multimap.impl.function;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;

import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.functional.EntryView;
import org.infinispan.multimap.impl.ExternalizerIds;

/**
 * Serializable function used by {@link org.infinispan.multimap.impl.EmbeddedMultimapCache#containsKey(Object)} and
 * {@link org.infinispan.multimap.impl.EmbeddedMultimapCache#containsEntry(Object, Object)}.
 *
 * @author Katia Aresti - karesti@redhat.com
 * @see <a href="http://infinispan.org/documentation/">Marshalling of Functions</a>
 * @since 9.2
 */
public final class ContainsFunction<K, V> implements BaseFunction<K, V, Boolean> {

   public static final AdvancedExternalizer<ContainsFunction> EXTERNALIZER = new Externalizer();
   private final V value;

   public ContainsFunction() {
      this.value = null;
   }

   /**
    * Call this constructor to create a function that checks if a key/value pair exists
    * <ul>
    * <li>if the key is null, the value will be searched in any key
    * <li>if the value is null, the key will be searched
    * <li>key-value pair are not null, the entry will be searched
    * <li>key-value pair are null, a {@link NullPointerException} will be raised
    * </ul>
    *
    * @param value value to be checked on the key
    */
   public ContainsFunction(V value) {
      this.value = value;
   }

   @Override
   public Boolean apply(EntryView.ReadWriteEntryView<K, Collection<V>> entryView) {
      Boolean contains;
      if (value == null) {
         contains = entryView.find().isPresent();
      } else {
         contains = entryView.find().map(values -> values.contains(value)).orElse(Boolean.FALSE);
      }

      return contains;
   }

   private static class Externalizer implements AdvancedExternalizer<ContainsFunction> {

      @Override
      public Set<Class<? extends ContainsFunction>> getTypeClasses() {
         return Collections.singleton(ContainsFunction.class);
      }

      @Override
      public Integer getId() {
         return ExternalizerIds.CONTAINS_KEY_VALUE_FUNCTION;
      }

      @Override
      public void writeObject(ObjectOutput output, ContainsFunction object) throws IOException {
         output.writeObject(object.value);
      }

      @Override
      public ContainsFunction readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return new ContainsFunction(input.readObject());
      }
   }
}

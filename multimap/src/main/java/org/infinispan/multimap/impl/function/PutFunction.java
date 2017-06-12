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
 * Serializable function used by {@link org.infinispan.multimap.impl.EmbeddedMultimapCache#put(Object, Object)}
 * to add a key/value pair.
 *
 * @author Katia Aresti - karesti@redhat.com
 * @see <a href="http://infinispan.org/documentation/">Marshalling of Functions</a>
 * @since 9.2
 */
public final class PutFunction<K, V> implements BaseFunction<K, V, Void> {

   public static final AdvancedExternalizer<PutFunction> EXTERNALIZER = new Externalizer();
   private final V value;

   public PutFunction(V value) {
      this.value = value;
   }

   @Override
   public Void apply(EntryView.ReadWriteEntryView<K, Collection<V>> entryView) {
      Collection<V> newValues = new HashSet<>();
      newValues.add(value);
      entryView.find().map(newValues::addAll);
      entryView.set(newValues);
      return null;
   }

   private static class Externalizer implements AdvancedExternalizer<PutFunction> {

      @Override
      public Set<Class<? extends PutFunction>> getTypeClasses() {
         return Collections.singleton(PutFunction.class);
      }

      @Override
      public Integer getId() {
         return ExternalizerIds.PUT_KEY_VALUE_FUNCTION;
      }

      @Override
      public void writeObject(ObjectOutput output, PutFunction object) throws IOException {
         output.writeObject(object.value);
      }

      @Override
      public PutFunction readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return new PutFunction(input.readObject());
      }
   }
}

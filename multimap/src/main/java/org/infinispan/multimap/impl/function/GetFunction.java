package org.infinispan.multimap.impl.function;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.functional.EntryView;
import org.infinispan.multimap.impl.ExternalizerIds;

/**
 * Serializable function used by {@link org.infinispan.multimap.impl.EmbeddedMultimapCache#get(Object)}
 * to get a key's value.
 *
 * @author Katia Aresti - karesti@redhat.com
 * @see <a href="http://infinispan.org/documentation/">Marshalling of Functions</a>
 * @since 9.2
 */
public final class GetFunction<K, V> implements BaseFunction<K, V, Collection<V>> {

   public static final AdvancedExternalizer<GetFunction> EXTERNALIZER = new Externalizer();

   @Override
   public Collection<V> apply(EntryView.ReadWriteEntryView<K, Collection<V>> entryView) {
      Optional<Collection<V>> valuesOpt = entryView.find();
      if (valuesOpt.isPresent()) {
         return new HashSet<>(entryView.find().get());
      } else {
         return Collections.emptySet();
      }
   }

   private static class Externalizer implements AdvancedExternalizer<GetFunction> {

      @Override
      public Set<Class<? extends GetFunction>> getTypeClasses() {
         return Collections.singleton(GetFunction.class);
      }

      @Override
      public Integer getId() {
         return ExternalizerIds.GET_FUNCTION;
      }

      @Override
      public void writeObject(ObjectOutput output, GetFunction object) throws IOException {
      }

      @Override
      public GetFunction readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return new GetFunction();
      }
   }
}

package org.infinispan.multimap.impl.function.sortedset;

import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.functional.EntryView;
import org.infinispan.multimap.impl.ExternalizerIds;
import org.infinispan.multimap.impl.SortedSetBucket;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;

/**
 * Serializable function used by
 * {@link org.infinispan.multimap.impl.EmbeddedMultimapSortedSetCache#score(Object, Object)}.
 *
 * @author Katia Aresti
 * @see <a href="http://infinispan.org/documentation/">Marshalling of Functions</a>
 * @since 15.0
 */
public final class ScoreFunction<K, V> implements SortedSetBucketBaseFunction<K, V, Double> {
   public static final AdvancedExternalizer<ScoreFunction> EXTERNALIZER = new Externalizer();
   private final V member;

   public ScoreFunction(V member) {
      this.member = member;
   }

   @Override
   public Double apply(EntryView.ReadWriteEntryView<K, SortedSetBucket<V>> entryView) {
      Optional<SortedSetBucket<V>> existing = entryView.peek();
      if (existing.isPresent()) {
         return existing.get().score(member);
      }
      return null;
   }

   private static class Externalizer implements AdvancedExternalizer<ScoreFunction> {

      @Override
      public Set<Class<? extends ScoreFunction>> getTypeClasses() {
         return Collections.singleton(ScoreFunction.class);
      }

      @Override
      public Integer getId() {
         return ExternalizerIds.SORTED_SET_SCORE_FUNCTION;
      }

      @Override
      public void writeObject(ObjectOutput output, ScoreFunction object) throws IOException {
         output.writeObject(object.member);
      }

      @Override
      public ScoreFunction readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return new ScoreFunction(input.readObject());
      }
   }
}

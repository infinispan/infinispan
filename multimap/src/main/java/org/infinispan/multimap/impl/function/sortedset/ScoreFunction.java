package org.infinispan.multimap.impl.function.sortedset;

import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.functional.EntryView;
import org.infinispan.multimap.impl.ExternalizerIds;
import org.infinispan.multimap.impl.SortedSetBucket;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
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
public final class ScoreFunction<K, V> implements SortedSetBucketBaseFunction<K, V, List<Double>> {
   public static final AdvancedExternalizer<ScoreFunction> EXTERNALIZER = new Externalizer();
   private final List<V> members;

   public ScoreFunction(List<V> members) {
      this.members = members;
   }

   @Override
   public List<Double> apply(EntryView.ReadWriteEntryView<K, SortedSetBucket<V>> entryView) {
      Optional<SortedSetBucket<V>> existing = entryView.peek();
      if (existing.isPresent()) {
         return existing.get().scores(members);
      }
      return Collections.emptyList();
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
         MarshallUtil.marshallCollection(object.members, output);
      }

      @Override
      public ScoreFunction readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return new ScoreFunction(MarshallUtil.unmarshallCollection(input, ArrayList::new));
      }
   }
}

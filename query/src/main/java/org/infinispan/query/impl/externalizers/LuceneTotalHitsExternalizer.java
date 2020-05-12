package org.infinispan.query.impl.externalizers;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Set;

import org.apache.lucene.search.TotalHits;
import org.infinispan.commons.io.UnsignedNumeric;
import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.marshall.MarshallUtil;

public class LuceneTotalHitsExternalizer extends AbstractExternalizer<TotalHits> {

   private static final TotalHits.Relation[] TOTALHITS_RELATION_VALUES = TotalHits.Relation.values();

   @Override
   public Integer getId() {
      return ExternalizerIds.LUCENE_TOTAL_HITS;
   }

   @Override
   public Set<Class<? extends TotalHits>> getTypeClasses() {
      return Collections.singleton(TotalHits.class);
   }

   @Override
   public void writeObject(ObjectOutput output, TotalHits totalHits) throws IOException {
      UnsignedNumeric.writeUnsignedLong(output, totalHits.value);
      MarshallUtil.marshallEnum(totalHits.relation, output);
   }

   @Override
   public TotalHits readObject(ObjectInput input) throws IOException, ClassNotFoundException {
      long value = UnsignedNumeric.readUnsignedLong(input);
      TotalHits.Relation relation = MarshallUtil.unmarshallEnum(input, (ordinal) -> TOTALHITS_RELATION_VALUES[ordinal]);
      return new TotalHits(value, relation);
   }
}

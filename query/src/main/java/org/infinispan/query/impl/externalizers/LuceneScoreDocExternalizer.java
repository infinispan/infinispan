package org.infinispan.query.impl.externalizers;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Set;

import org.apache.lucene.search.ScoreDoc;
import org.infinispan.commons.io.UnsignedNumeric;
import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.util.Util;

public class LuceneScoreDocExternalizer extends AbstractExternalizer<ScoreDoc> {

   @Override
   public Set<Class<? extends ScoreDoc>> getTypeClasses() {
      return Util.<Class<? extends ScoreDoc>>asSet(ScoreDoc.class);
   }

   @Override
   public ScoreDoc readObject(final ObjectInput input) throws IOException, ClassNotFoundException {
      return readObjectStatic(input);
   }

   @Override
   public void writeObject(final ObjectOutput output, final ScoreDoc sortField) throws IOException {
      writeObjectStatic(output, sortField);
   }

   @Override
   public Integer getId() {
      return ExternalizerIds.LUCENE_SCORE_DOC;
   }

   private static void writeObjectStatic(final ObjectOutput output, final ScoreDoc sortField) throws IOException {
      output.writeFloat(sortField.score);
      UnsignedNumeric.writeUnsignedInt(output, sortField.doc);
      output.writeInt(sortField.shardIndex);
   }

   private  static ScoreDoc readObjectStatic(final ObjectInput input) throws IOException, ClassNotFoundException {
      final float score = input.readFloat();
      final int doc = UnsignedNumeric.readUnsignedInt(input);
      final int shardId = input.readInt();
      return new ScoreDoc(doc, score, shardId);
   }
}

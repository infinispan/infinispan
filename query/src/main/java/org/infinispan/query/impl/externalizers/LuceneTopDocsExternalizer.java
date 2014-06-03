package org.infinispan.query.impl.externalizers;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Set;

import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.infinispan.commons.io.UnsignedNumeric;
import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.util.Util;

public class LuceneTopDocsExternalizer extends AbstractExternalizer<TopDocs> {

   @Override
   public Set<Class<? extends TopDocs>> getTypeClasses() {
      return Util.<Class<? extends TopDocs>>asSet(TopDocs.class);
   }

   @Override
   public TopDocs readObject(final ObjectInput input) throws IOException, ClassNotFoundException {
      return readObjectStatic(input);
   }

   @Override
   public void writeObject(final ObjectOutput output, final TopDocs topDocs) throws IOException {
      writeObjectStatic(output, topDocs);
   }

   @Override
   public Integer getId() {
      return ExternalizerIds.LUCENE_TOPDOCS;
   }

   static void writeObjectStatic(final ObjectOutput output, final TopDocs topDocs) throws IOException {
      UnsignedNumeric.writeUnsignedInt(output, topDocs.totalHits);
      output.writeFloat(topDocs.getMaxScore());
      final ScoreDoc[] scoreDocs = topDocs.scoreDocs;
      final int count = scoreDocs.length;
      UnsignedNumeric.writeUnsignedInt(output, count);
      for (int i=0; i<count; i++) {
         output.writeObject(scoreDocs[i]);
      }
   }

   static TopDocs readObjectStatic(final ObjectInput input) throws IOException, ClassNotFoundException {
      final int totalHits = UnsignedNumeric.readUnsignedInt(input);
      final float maxScore = input.readFloat();
      final int scoreCount = UnsignedNumeric.readUnsignedInt(input);
      final ScoreDoc[] scoreDocs = new ScoreDoc[scoreCount];
      for (int i=0; i<scoreCount; i++) {
         scoreDocs[i] = (ScoreDoc) input.readObject();
      }
      return new TopDocs(totalHits, scoreDocs, maxScore);
   }
}

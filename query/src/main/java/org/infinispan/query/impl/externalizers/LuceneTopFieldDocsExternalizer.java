package org.infinispan.query.impl.externalizers;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Set;

import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopFieldDocs;
import org.infinispan.commons.io.UnsignedNumeric;
import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.util.Util;

public class LuceneTopFieldDocsExternalizer extends AbstractExternalizer<TopFieldDocs> {

   @Override
   public Set<Class<? extends TopFieldDocs>> getTypeClasses() {
      return Util.<Class<? extends TopFieldDocs>>asSet(TopFieldDocs.class);
   }

   @Override
   public TopFieldDocs readObject(final ObjectInput input) throws IOException, ClassNotFoundException {
      final int totalHits = UnsignedNumeric.readUnsignedInt(input);
      final float maxScore = input.readFloat();
      final int sortFieldsCount = UnsignedNumeric.readUnsignedInt(input);
      final SortField[] sortFields = new SortField[sortFieldsCount];
      for (int i=0; i<sortFieldsCount; i++) {
         sortFields[i] = LuceneSortFieldExternalizer.readObjectStatic(input);
      }
      final int scoreDocsCount = UnsignedNumeric.readUnsignedInt(input);
      final ScoreDoc[] scoreDocs = new ScoreDoc[scoreDocsCount];
      for (int i=0; i<scoreDocsCount; i++) {
         scoreDocs[i] = (ScoreDoc) input.readObject();
      }
      return new TopFieldDocs(totalHits, scoreDocs, sortFields, maxScore);
   }

   @Override
   public void writeObject(final ObjectOutput output, final TopFieldDocs topFieldDocs) throws IOException {
      UnsignedNumeric.writeUnsignedInt(output, topFieldDocs.totalHits);
      output.writeFloat(topFieldDocs.getMaxScore());
      final SortField[] sortFields = topFieldDocs.fields;
      UnsignedNumeric.writeUnsignedInt(output, sortFields.length);
      for (SortField sortField : sortFields) {
         LuceneSortFieldExternalizer.writeObjectStatic(output, sortField);
      }
      final ScoreDoc[] scoreDocs = topFieldDocs.scoreDocs;
      UnsignedNumeric.writeUnsignedInt(output, scoreDocs.length);
      for (ScoreDoc scoreDoc : scoreDocs) {
         output.writeObject(scoreDoc);
      }
   }

   @Override
   public Integer getId() {
      return ExternalizerIds.LUCENE_TOPFIELDDOCS;
   }
}

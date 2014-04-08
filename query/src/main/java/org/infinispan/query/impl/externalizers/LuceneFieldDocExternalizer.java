package org.infinispan.query.impl.externalizers;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Set;

import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.ScoreDoc;
import org.infinispan.commons.io.UnsignedNumeric;
import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.util.Util;

public class LuceneFieldDocExternalizer extends AbstractExternalizer<FieldDoc> {

   @Override
   public Set<Class<? extends FieldDoc>> getTypeClasses() {
      return Util.<Class<? extends FieldDoc>>asSet(FieldDoc.class);
   }

   @Override
   public FieldDoc readObject(final ObjectInput input) throws IOException, ClassNotFoundException {
      return readObjectStatic(input);
   }

   @Override
   public void writeObject(final ObjectOutput output, final FieldDoc sortField) throws IOException {
      writeObjectStatic(output, sortField);
   }

   @Override
   public Integer getId() {
      return ExternalizerIds.LUCENE_FIELD_SCORE_DOC;
   }

   static void writeObjectStatic(final ObjectOutput output, final FieldDoc sortField) throws IOException {
      output.writeFloat(sortField.score);
      UnsignedNumeric.writeUnsignedInt(output, sortField.doc);
      output.writeInt(sortField.shardIndex);
      final Object[] fields = sortField.fields;
      UnsignedNumeric.writeUnsignedInt(output, fields.length);
      for (int i=0; i<fields.length; i++) {
         output.writeObject(fields[i]);
      }
   }

   public static FieldDoc readObjectStatic(final ObjectInput input) throws IOException, ClassNotFoundException {
      final float score = input.readFloat();
      final int doc = UnsignedNumeric.readUnsignedInt(input);
      final int shardId = input.readInt();
      final int fieldsArrayLenght = UnsignedNumeric.readUnsignedInt(input);
      Object[] fields = new Object[fieldsArrayLenght];
      for (int i=0; i<fieldsArrayLenght; i++) {
         fields[i] = input.readObject();
      }
      return new FieldDoc(doc, score, fields, shardId);
   }

}

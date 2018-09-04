package org.infinispan.query.impl.externalizers;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Set;

import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortField.Type;
import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.marshall.MarshallUtil;


/**
 * WARNING: this Externalizer implementation drops some state associated to the SortField instance.
 *
 * A CUSTOM Sort Type is unsupported, and it is also not possible to use a custom Field Parser
 * or options related to missing value sorting.
 */
public class LuceneSortFieldExternalizer extends AbstractExternalizer<SortField> {

   private static final SortField.Type[] SORTFIELD_TYPE_VALUES = SortField.Type.values();

   @Override
   public Set<Class<? extends SortField>> getTypeClasses() {
      return Collections.singleton(SortField.class);
   }

   @Override
   public SortField readObject(ObjectInput input) throws IOException {
      return readObjectStatic(input);
   }

   @Override
   public void writeObject(ObjectOutput output, SortField sortField) throws IOException {
      writeObjectStatic(output, sortField);
   }

   @Override
   public Integer getId() {
      return ExternalizerIds.LUCENE_SORT_FIELD;
   }

   static void writeObjectStatic(ObjectOutput output, SortField sortField) throws IOException {
      output.writeUTF(sortField.getField());
      MarshallUtil.marshallEnum(sortField.getType(), output);
      output.writeBoolean(sortField.getReverse());
   }

   static SortField readObjectStatic(ObjectInput input) throws IOException {
      String fieldName = input.readUTF();
      Type sortType = MarshallUtil.unmarshallEnum(input, (ordinal) -> SORTFIELD_TYPE_VALUES[ordinal]);
      boolean reverseSort = input.readBoolean();
      return new SortField(fieldName, sortType, reverseSort);
   }
}

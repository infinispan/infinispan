package org.infinispan.query.impl.externalizers;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Set;

import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortField.Type;
import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.util.Util;


/**
 * WARNING: this Externalizer implementation drops some state associated to the SortField instance.
 *
 * A CUSTOM Sort Type is unsupported, and it is also not possible to use a custom Field Parser
 * or options related to missing value sorting.
 */
public class LuceneSortFieldExternalizer extends AbstractExternalizer<SortField> {

   @Override
   public Set<Class<? extends SortField>> getTypeClasses() {
      return Util.<Class<? extends SortField>>asSet(SortField.class);
   }

   @Override
   public SortField readObject(final ObjectInput input) throws IOException, ClassNotFoundException {
      return readObjectStatic(input);
   }

   @Override
   public void writeObject(final ObjectOutput output, final SortField sortField) throws IOException {
      writeObjectStatic(output, sortField);
   }

   @Override
   public Integer getId() {
      return ExternalizerIds.LUCENE_SORT_FIELD;
   }

   static void writeObjectStatic(final ObjectOutput output, final SortField sortField) throws IOException {
      output.writeUTF(sortField.getField());
      output.writeObject(sortField.getType());
      output.writeBoolean(sortField.getReverse());
   }

   public static SortField readObjectStatic(final ObjectInput input) throws IOException, ClassNotFoundException {
      final String fieldName = input.readUTF();
      final Type sortType = (Type) input.readObject();
      final boolean reverseSort = input.readBoolean();
      return new SortField(fieldName, sortType, reverseSort);
   }

}

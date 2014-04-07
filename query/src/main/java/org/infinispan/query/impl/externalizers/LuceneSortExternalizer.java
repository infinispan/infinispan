package org.infinispan.query.impl.externalizers;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Set;

import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.infinispan.commons.io.UnsignedNumeric;
import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.util.Util;


public class LuceneSortExternalizer extends AbstractExternalizer<Sort> {

   @Override
   public Set<Class<? extends Sort>> getTypeClasses() {
      return Util.<Class<? extends Sort>>asSet(Sort.class);
   }

   @Override
   public Sort readObject(final ObjectInput input) throws IOException, ClassNotFoundException {
      final int count = UnsignedNumeric.readUnsignedInt(input);
      SortField[] sortfields = new SortField[count];
      for (int i=0; i<count; i++) {
         sortfields[i] = LuceneSortFieldExternalizer.readObjectStatic(input);
      }
      Sort sort = new Sort();
      sort.setSort(sortfields);
      return sort;
   }

   @Override
   public void writeObject(final ObjectOutput output, final Sort sort) throws IOException {
      final SortField[] sortFields = sort.getSort();
      final int count = sortFields.length;
      UnsignedNumeric.writeUnsignedInt(output, count);
      for (int i=0; i<count; i++) {
         LuceneSortFieldExternalizer.writeObjectStatic(output, sortFields[i]);
      }
   }

   @Override
   public Integer getId() {
      return ExternalizerIds.LUCENE_SORT;
   }

}

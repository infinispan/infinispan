package org.infinispan.lucene.impl;

import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.commons.util.Util;
import org.infinispan.lucene.ExternalizerIds;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Set;

/**
 * Add operation associated to {@link org.infinispan.lucene.impl.FileListCacheValueDelta}
 *
 * @author gustavonalle
 * @since 7.0
 */
public class AddOperation implements Operation {

   private final String element;

   AddOperation(String element) {
      this.element = element;
   }

   @Override
   public void apply(Set<String> target) {
      target.add(element);
   }

   String getElement() {
      return element;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      AddOperation that = (AddOperation) o;

      return element.equals(that.element);
   }

   @Override
   public int hashCode() {
      return element.hashCode();
   }

   public static class AddOperationExternalizer implements AdvancedExternalizer<AddOperation> {

      @Override
      public void writeObject(ObjectOutput output, AddOperation addOperation) throws IOException {
         output.writeUTF(addOperation.element);
      }

      @Override
      public AddOperation readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return new AddOperation(input.readUTF());
      }

      @Override
      @SuppressWarnings("unchecked")
      public Set<Class<? extends AddOperation>> getTypeClasses() {
         return Util.<Class<? extends AddOperation>>asSet(AddOperation.class);
      }

      @Override
      public Integer getId() {
         return ExternalizerIds.FILE_LIST_DELTA_ADD;
      }
   }
}

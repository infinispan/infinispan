package org.infinispan.lucene.impl;

import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.commons.util.Util;
import org.infinispan.lucene.ExternalizerIds;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Set;

/**
 * Delete operation associated to {@link org.infinispan.lucene.impl.FileListCacheValueDelta}
 *
 * @author gustavonalle
 * @since 7.0
 */
public class DeleteOperation implements Operation {

   private String element;

   DeleteOperation(String element) {
      this.element = element;
   }

   @Override
   public void apply(Set<String> target) {
      target.remove(element);
   }

   String getElement() {
      return element;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      DeleteOperation that = (DeleteOperation) o;

      return element.equals(that.element);
   }

   @Override
   public int hashCode() {
      return element.hashCode();
   }

   public static class DeleteElementOperationExternalizer implements AdvancedExternalizer<DeleteOperation> {

      @Override
      @SuppressWarnings("unchecked")
      public Set<Class<? extends DeleteOperation>> getTypeClasses() {
         return Util.<Class<? extends DeleteOperation>>asSet(DeleteOperation.class);
      }

      @Override
      public void writeObject(ObjectOutput output, DeleteOperation deleteOperation) throws IOException {
         output.writeUTF(deleteOperation.element);
      }

      @Override
      public DeleteOperation readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return new DeleteOperation(input.readUTF());
      }

      @Override
      public Integer getId() {
         return ExternalizerIds.FILE_LIST_DELTA_DEL;
      }

   }
}

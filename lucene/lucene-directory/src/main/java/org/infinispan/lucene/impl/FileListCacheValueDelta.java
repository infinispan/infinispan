package org.infinispan.lucene.impl;

import org.infinispan.atomic.Delta;
import org.infinispan.atomic.DeltaAware;
import org.infinispan.commons.io.UnsignedNumeric;
import org.infinispan.commons.marshall.AbstractExternalizer;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * {@link org.infinispan.atomic.Delta} implementation for {@link org.infinispan.lucene.impl.FileListCacheValue}
 *
 * @author gustavonalle
 * @since 7.0
 */
public class FileListCacheValueDelta implements Delta {

   private final List<Operation> ops;

   FileListCacheValueDelta(List<Operation> ops) {
      this.ops = ops;
   }

   FileListCacheValueDelta() {
      this.ops = new ArrayList<>();
   }

   @Override
   public DeltaAware merge(DeltaAware deltaAware) {
      FileListCacheValue other;
      if (deltaAware instanceof FileListCacheValue) {
         other = (FileListCacheValue) deltaAware;
      } else {
         other = new FileListCacheValue();
      }
      other.apply(ops);
      return other;
   }

   List<Operation> getOps() {
      return ops;
   }

   void addOperation(String element) {
      ops.add(new AddOperation(element));
   }

   void removeOperation(String element) {
      AddOperation addOp = new AddOperation(element);
      if (ops.contains(addOp)) {
         ops.remove(addOp);
      } else {
         ops.add(new DeleteOperation(element));
      }
   }

   void discardOps() {
      ops.clear();
   }


   public static final class Externalizer extends AbstractExternalizer<FileListCacheValueDelta> {

      @Override
      public Set<Class<? extends FileListCacheValueDelta>> getTypeClasses() {
         Set<Class<? extends FileListCacheValueDelta>> classes = new HashSet<>();
         classes.add(FileListCacheValueDelta.class);
         return classes;
      }

      @Override
      public void writeObject(ObjectOutput output, FileListCacheValueDelta object) throws IOException {
         int size = object.ops.size();
         UnsignedNumeric.writeUnsignedInt(output, size);
         for (int i = 0; i < size; i++) {
            output.writeObject(object.ops.get(i));
         }
      }

      @Override
      public FileListCacheValueDelta readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         int size = UnsignedNumeric.readUnsignedInt(input);
         ArrayList<Operation> operations = new ArrayList<>(size);
         for (int i = 0; i < size; i++) {
            operations.add((Operation) input.readObject());
         }
         return new FileListCacheValueDelta(operations);
      }
   }

}

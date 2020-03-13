package org.infinispan.commons.marshall;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Set;
import java.util.function.Supplier;

import org.infinispan.commons.util.Util;

public class SuppliedExternalizer<T> implements AdvancedExternalizer<T> {
   private final Integer id;
   private final Class<T> clazz;
   private final Supplier<T> supplier;

   public SuppliedExternalizer(Integer id, Class<T> clazz, Supplier<T> supplier) {
      this.id = id;
      this.clazz = clazz;
      this.supplier = supplier;
   }

   @Override
   public Set<Class<? extends T>> getTypeClasses() {
      return Util.asSet(clazz);
   }

   @Override
   public Integer getId() {
      return id;
   }

   @Override
   public void writeObject(ObjectOutput output, T object) throws IOException {
   }

   @Override
   public T readObject(ObjectInput input) throws IOException, ClassNotFoundException {
      return supplier.get();
   }
}

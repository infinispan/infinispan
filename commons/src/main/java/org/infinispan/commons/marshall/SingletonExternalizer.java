package org.infinispan.commons.marshall;

import java.io.IOException;
import java.util.Set;

import org.infinispan.commons.util.Util;

public class SingletonExternalizer<T> implements AdvancedExternalizer<T> {
   private final Integer id;
   private final T instance;

   public SingletonExternalizer(Integer id, T instance) {
      this.id = id;
      this.instance = instance;
   }

   @Override
   public Set<Class<? extends T>> getTypeClasses() {
      Class<T> clazz = (Class<T>) instance.getClass();
      return Util.asSet(clazz);
   }

   @Override
   public Integer getId() {
      return id;
   }

   @Override
   public void writeObject(UserObjectOutput output, T object) throws IOException {
   }

   @Override
   public T readObject(UserObjectInput input) throws IOException, ClassNotFoundException {
      return instance;
   }
}

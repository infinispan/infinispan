package org.infinispan.tools.store.migrator.marshaller.common;

import java.io.IOException;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Set;

public abstract class AbstractMigratorExternalizer<T> implements AdvancedExternalizer<T> {

   Set<Class<? extends T>> classes;
   Integer id;

   public AbstractMigratorExternalizer(Class<? extends T> clazz, Integer id) {
      this(Collections.singleton(clazz), id);
   }

   public AbstractMigratorExternalizer(Set<Class<? extends T>> classes, Integer id) {
      this.classes = classes;
      this.id = id;
   }

   @Override
   public Integer getId() {
      return id;
   }

   @Override
   public Set<Class<? extends T>> getTypeClasses() {
      return classes;
   }

   @Override
   public void writeObject(ObjectOutput output, T object) throws IOException {
      throw new IllegalStateException(String.format("writeObject called on Externalizer %s", this.getClass().getSimpleName()));
   }
}

package org.infinispan.jcache.embedded.functions;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Set;

import javax.cache.processor.MutableEntry;

import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.commons.util.ReflectionUtil;
import org.infinispan.commons.util.Util;
import org.infinispan.jcache.embedded.ExternalizerIds;

public class MutableEntrySnapshot<K, V> implements MutableEntry<K, V> {
   private final K key;
   private final V value;

   public MutableEntrySnapshot(K key, V value) {
      this.key = key;
      this.value = value;
   }

   @Override
   public boolean exists() {
      return value != null;
   }

   @Override
   public void remove() {
      throw new UnsupportedOperationException();
   }

   @Override
   public K getKey() {
      return key;
   }

   @Override
   public V getValue() {
      return value;
   }

   @Override
   public <T> T unwrap(Class<T> clazz) {
      return ReflectionUtil.unwrap(this, clazz);
   }

   @Override
   public void setValue(V value) {
      throw new UnsupportedOperationException();
   }

   // This externalizer may not be registered if JCache is not on the classpath!
   public static class Externalizer implements AdvancedExternalizer<MutableEntrySnapshot> {
      @Override
      public Set<Class<? extends MutableEntrySnapshot>> getTypeClasses() {
         return Util.asSet(MutableEntrySnapshot.class);
      }

      @Override
      public Integer getId() {
         return ExternalizerIds.MUTABLE_ENTRY_SNAPSHOT;
      }

      @Override
      public void writeObject(ObjectOutput output, MutableEntrySnapshot object) throws IOException {
         output.writeObject(object.key);
         output.writeObject(object.value);
      }

      @Override
      public MutableEntrySnapshot readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return new MutableEntrySnapshot(input.readObject(), input.readObject());
      }
   }
}

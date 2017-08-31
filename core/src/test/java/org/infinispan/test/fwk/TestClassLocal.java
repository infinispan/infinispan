package org.infinispan.test.fwk;

import java.io.ObjectStreamException;
import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.infinispan.test.AbstractInfinispanTest;

/**
 * @author Dan Berindei
 * @since 9.1
 */
public class TestClassLocal<T> implements Serializable {
   private static final Map<String, TestClassLocal> values = new ConcurrentHashMap<>();

   private final String name;
   private final AbstractInfinispanTest test;
   private final Supplier<T> supplier;
   private final Consumer<T> destroyer;
   private Object value;

   public TestClassLocal(String name, AbstractInfinispanTest test, Supplier<T> supplier, Consumer<T> destroyer) {
      this.name = name;
      this.test = test;
      this.supplier = supplier;
      this.destroyer = destroyer;
      values.put(id(), this);
   }

   @SuppressWarnings("unchecked")
   public T get() {
      synchronized (this) {
         if (value == null) {
            value = supplier.get();
            TestResourceTracker.addResource(test, new TestResourceTracker.Cleaner<TestClassLocal<T>>(this) {
               @Override
               public void close() {
                  ref.destroyer.accept(ref.get());
               }
            });
         }
         return (T) value;
      }
   }

   @SuppressWarnings("unchecked")
   void close() {
      T value = (T) values.remove(this);
      destroyer.accept(value);
   }

   public String id() {
      return name + "/" + test.toString();
   }

   @Override
   public String toString() {
      return id();
   }

   Object writeReplace() throws ObjectStreamException {
      return new Token(id());
   }

   static class Token implements Serializable {
      private String id;

      Token(String id) {
         this.id = id;
      }

      Object readResolve() throws ObjectStreamException {
         return values.get(id);
      }
   }
}

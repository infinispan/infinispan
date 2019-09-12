package org.infinispan.test.fwk;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.infinispan.commons.marshall.SerializeWith;
import org.infinispan.test.AbstractInfinispanTest;

/**
 * @author Dan Berindei
 * @since 9.1
 */
@SerializeWith(TestClassLocal.Externalizer.class)
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

   public static class Externalizer implements org.infinispan.commons.marshall.Externalizer<TestClassLocal> {

      @Override
      public void writeObject(ObjectOutput output, TestClassLocal object) throws IOException {
         output.writeUTF(object.id());
      }

      @Override
      public TestClassLocal readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         String id = input.readUTF();
         return values.get(id);
      }
   }
}

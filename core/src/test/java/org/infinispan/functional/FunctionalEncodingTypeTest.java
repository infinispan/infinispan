package org.infinispan.functional;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;

import java.io.Serializable;

import javax.transaction.Status;
import javax.transaction.TransactionManager;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.dataconversion.Encoder;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.functional.impl.FunctionalMapImpl;
import org.infinispan.functional.impl.ReadWriteMapImpl;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.marshall.core.EncoderRegistry;
import org.infinispan.marshall.core.ExternalPojo;
import org.infinispan.test.fwk.InTransactionMode;
import org.infinispan.transaction.TransactionMode;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "functional.FunctionalEncoderTest")
public class FunctionalEncodingTypeTest extends FunctionalMapTest {

   @Override
   public Object[] factory() {
      return new Object[] {
            new FunctionalEncodingTypeTest().transactional(false),
            new FunctionalEncodingTypeTest().transactional(true)
      };
   }

   @Override
   protected AdvancedCache<?, ?> getAdvancedCache(EmbeddedCacheManager cm, String cacheName) {
      AdvancedCache<?, ?> cache = super.getAdvancedCache(cm, cacheName);
      ensureEncoders(cache);
      return cache.withEncoding(TestKeyEncoder.class, TestValueEncoder.class);
   }

   private void ensureEncoders(AdvancedCache<?, ?> cache) {
      EncoderRegistry encoderRegistry = cache.getComponentRegistry().getComponent(EncoderRegistry.class);
      if (!encoderRegistry.isRegistered(TestKeyEncoder.class)) {
         encoderRegistry.registerEncoder(new TestKeyEncoder());
         encoderRegistry.registerEncoder(new TestValueEncoder());
         encoderRegistry.registerEncoder(new UserType1Encoder());
         encoderRegistry.registerEncoder(new UserType2Encoder());
      }
   }

   @InTransactionMode(TransactionMode.TRANSACTIONAL)
   public void testDifferentEncoders() throws Exception {
      AdvancedCache<UserType<String>, UserType<String>> ac1 = (AdvancedCache<UserType<String>, UserType<String>>) advancedCache(0).withEncoding(UserType1Encoder.class, UserType1Encoder.class);
      AdvancedCache<UserType<String>, UserType<String>> ac2 = (AdvancedCache<UserType<String>, UserType<String>>) advancedCache(0).withEncoding(UserType2Encoder.class, UserType2Encoder.class);
      FunctionalMap.ReadWriteMap<UserType<String>, UserType<String>> rw1 = ReadWriteMapImpl.create(FunctionalMapImpl.create(ac1));
      FunctionalMap.ReadWriteMap<UserType<String>, UserType<String>> rw2 = ReadWriteMapImpl.create(FunctionalMapImpl.create(ac2));

      assertEquals(0, ac1.size());
      TransactionManager tm = tm(0);
      tm.begin();
      try {
         rw1.eval(new UserType<>(1, "key"), view -> {
            assertFalse(view.find().isPresent());
            view.set(new UserType<>(1, "value"));
            return null;
         });
         rw2.eval(new UserType<>(2, "key"), view -> {
            UserType<String> value = view.find().orElseThrow(() -> new AssertionError());
            assertEquals(2, value.type);
            assertEquals("value", value.instance);
            view.set(new UserType<>(2, "value2"));
            return null;
         });
         UserType<String> value2 = ac1.get(new UserType<>(1, "key"));
         assertEquals(1, value2.type);
         assertEquals("value2", value2.instance);
      } finally {
         if (tm.getStatus() == Status.STATUS_ACTIVE) {
            tm.commit();
         } else {
            tm.rollback();
         }
      }
      assertEquals(1, ac1.size());
      UserType<String> value2 = ac2.get(new UserType<>(2, "key"));
      assertEquals(2, value2.type);
      assertEquals("value2", value2.instance);
   }

   static class TestKeyEncoder implements Encoder {
      private static final short ID = 42;

      @Override
      public Object toStorage(Object content) {
         return content == null ? null : new Wrapper(content);
      }

      @Override
      public Object fromStorage(Object content) {
         return content == null ? null : ((Wrapper) content).content;
      }

      @Override
      public boolean isStorageFormatFilterable() {
         return false;
      }

      @Override
      public MediaType getStorageFormat() {
         return MediaType.APPLICATION_OBJECT;
      }

      @Override
      public short id() {
         return ID;
      }

      static class Wrapper implements Serializable, ExternalPojo {
         private final Object content;

         Wrapper(Object content) {
            this.content = content;
         }

         @Override
         public String toString() {
            return "TestKey#" + content;
         }

         @Override
         public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Wrapper wrapper = (Wrapper) o;

            return content.equals(wrapper.content);
         }

         @Override
         public int hashCode() {
            return content.hashCode();
         }
      }
   }

   static class TestValueEncoder implements Encoder {

      private static final short ID = 43;

      @Override
      public Object toStorage(Object content) {
         return content == null ? null : new Wrapper(content);
      }

      @Override
      public Object fromStorage(Object content) {
         return content == null ? null : ((Wrapper) content).content;
      }

      @Override
      public boolean isStorageFormatFilterable() {
         return false;
      }

      @Override
      public MediaType getStorageFormat() {
         return MediaType.APPLICATION_OBJECT;
      }

      @Override
      public short id() {
         return ID;
      }

      static class Wrapper implements Serializable, ExternalPojo {
         private final Object content;

         Wrapper(Object content) {
            this.content = content;
         }

         @Override
         public String toString() {
            return "TestValue#" + content;
         }

         @Override
         public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Wrapper wrapper = (Wrapper) o;

            return content.equals(wrapper.content);
         }

         @Override
         public int hashCode() {
            return content.hashCode();
         }
      }
   }

   // Not serializable!
   static final class UserType<T> {
      public final int type;
      public final T instance;

      UserType(int type, T instance) {
         this.type = type;
         this.instance = instance;
      }

      @Override
      public boolean equals(Object o) {
         if (this == o) return true;
         if (o == null || getClass() != o.getClass()) return false;

         UserType<?> userType = (UserType<?>) o;
         return type == userType.type && instance.equals(userType.instance);
      }

      @Override
      public int hashCode() {
         return instance.hashCode();
      }

      @Override
      public String toString() {
         return "UserType#" + type + "#" + instance;
      }
   }

   static class UserType1Encoder implements Encoder {
      @Override
      public Object toStorage(Object content) {
         UserType<?> ut = (UserType<?>) content;
         assert ut.type == 1;
         return content == null ? null : ut.instance;
      }

      @Override
      public Object fromStorage(Object content) {
         return content == null ? null : new UserType<>(1, content);
      }

      @Override
      public boolean isStorageFormatFilterable() {
         return false;
      }

      @Override
      public MediaType getStorageFormat() {
         return MediaType.APPLICATION_OBJECT;
      }

      @Override
      public short id() {
         return (short) 44;
      }
   }

   static class UserType2Encoder implements Encoder {
      @Override
      public Object toStorage(Object content) {
         UserType<?> ut = (UserType<?>) content;
         assert ut.type == 2;
         return content == null ? null : ut.instance;
      }

      @Override
      public Object fromStorage(Object content) {
         return content == null ? null : new UserType<>(1, content);
      }

      @Override
      public boolean isStorageFormatFilterable() {
         return false;
      }

      @Override
      public MediaType getStorageFormat() {
         return MediaType.APPLICATION_OBJECT;
      }

      @Override
      public short id() {
         return (short) 45;
      }
   }
}

package org.infinispan.marshall;

import java.io.IOException;
import java.io.ObjectInput;
import java.lang.reflect.Method;
import java.util.Date;
import java.util.Set;

import org.infinispan.Cache;
import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.commons.marshall.UserObjectOutput;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.CacheContainer;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

/**
 * Tests configuration of user defined {@link AdvancedExternalizer} implementations
 *
 * @author Galder Zamarreño
 * @since 5.0
 */
@Test(groups = "functional", testName = "marshall.AdvancedExternalizerTest")
public class AdvancedExternalizerTest extends MultipleCacheManagersTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      GlobalConfigurationBuilder globalCfg1 = createForeignExternalizerGlobalConfig();
      GlobalConfigurationBuilder globalCfg2 = createForeignExternalizerGlobalConfig();
      ConfigurationBuilder cfg = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, false);
      CacheContainer cm1 = TestCacheManagerFactory.createClusteredCacheManager(globalCfg1, cfg);
      CacheContainer cm2 = TestCacheManagerFactory.createClusteredCacheManager(globalCfg2, cfg);
      registerCacheManager(cm1, cm2);
      defineConfigurationOnAllManagers(getCacheName(), cfg);
      waitForClusterToForm(getCacheName());
   }

   protected String getCacheName() {
      return "ForeignExternalizers";
   }

   protected GlobalConfigurationBuilder createForeignExternalizerGlobalConfig() {
      GlobalConfigurationBuilder builder = new GlobalConfigurationBuilder().clusteredDefault();
      builder.serialization()
         .addAdvancedExternalizer(1234, new IdViaConfigObj.Externalizer())
         .addAdvancedExternalizer(new IdViaAnnotationObj.Externalizer())
         .addAdvancedExternalizer(3456, new IdViaBothObj.Externalizer());
      return builder;
   }

   public void testReplicatePojosWithUserDefinedExternalizers(Method m) {
      Cache cache1 = manager(0).getCache(getCacheName());
      Cache cache2 = manager(1).getCache(getCacheName());
      IdViaConfigObj configObj = new IdViaConfigObj().setName("Galder");
      String key = "k-" + m.getName() + "-viaConfig";
      cache1.put(key, configObj);
      assert configObj.name.equals(((IdViaConfigObj)cache2.get(key)).name);
      IdViaAnnotationObj annotationObj = new IdViaAnnotationObj().setDate(new Date(System.currentTimeMillis()));
      key = "k-" + m.getName() + "-viaAnnotation";
      cache1.put(key, annotationObj);
      assert annotationObj.date.equals(((IdViaAnnotationObj)cache2.get(key)).date);
      IdViaBothObj bothObj = new IdViaBothObj().setAge(30);
      key = "k-" + m.getName() + "-viaBoth";
      cache1.put(key, bothObj);
      assert bothObj.age == ((IdViaBothObj)cache2.get(key)).age;
   }

   public static class IdViaConfigObj {
      String name;

      public IdViaConfigObj setName(String name) {
         this.name = name;
         return this;
      }

      public static class Externalizer extends AbstractExternalizer<IdViaConfigObj> {
         @Override
         public void writeObject(UserObjectOutput output, IdViaConfigObj object) throws IOException {
            output.writeUTF(object.name);
         }

         @Override
         public IdViaConfigObj readObject(ObjectInput input) throws IOException, ClassNotFoundException {
            return new IdViaConfigObj().setName(input.readUTF());
         }

         @Override
         public Set<Class<? extends IdViaConfigObj>> getTypeClasses() {
            return Util.<Class<? extends IdViaConfigObj>>asSet(IdViaConfigObj.class);
         }
      }
   }

   public static class IdViaAnnotationObj {
      Date date;

      public IdViaAnnotationObj setDate(Date date) {
         this.date = date;
         return this;
      }

      public static class Externalizer extends AbstractExternalizer<IdViaAnnotationObj> {
         @Override
         public void writeObject(UserObjectOutput output, IdViaAnnotationObj object) throws IOException {
            output.writeObject(object.date);
         }

         @Override
         public IdViaAnnotationObj readObject(ObjectInput input) throws IOException, ClassNotFoundException {
            return new IdViaAnnotationObj().setDate((Date) input.readObject());
         }

         @Override
         public Integer getId() {
            return 5678;
         }

         @Override
         public Set<Class<? extends IdViaAnnotationObj>> getTypeClasses() {
            return Util.<Class<? extends IdViaAnnotationObj>>asSet(IdViaAnnotationObj.class);
         }
      }
   }

   public static class IdViaBothObj {
      int age;

      public IdViaBothObj setAge(int age) {
         this.age = age;
         return this;
      }

      public static class Externalizer extends AbstractExternalizer<IdViaBothObj> {
         @Override
         public void writeObject(UserObjectOutput output, IdViaBothObj object) throws IOException {
            output.writeInt(object.age);
         }

         @Override
         public IdViaBothObj readObject(ObjectInput input) throws IOException, ClassNotFoundException {
            return new IdViaBothObj().setAge(input.readInt());
         }

         @Override
         public Integer getId() {
            return 9012;
         }

         @Override
         public Set<Class<? extends IdViaBothObj>> getTypeClasses() {
            return Util.<Class<? extends IdViaBothObj>>asSet(IdViaBothObj.class);
         }
      }
   }

}

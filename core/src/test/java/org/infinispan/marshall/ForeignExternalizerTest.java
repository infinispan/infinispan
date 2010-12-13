package org.infinispan.marshall;

import org.infinispan.Cache;
import org.infinispan.config.Configuration;
import org.infinispan.config.ExternalizerConfig;
import org.infinispan.config.GlobalConfiguration;
import org.infinispan.config.GlobalConfiguration.ExternalizersType;
import org.infinispan.manager.CacheContainer;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Tests configuration of user defined {@link Externalizer} implementations
 *
 * @author Galder Zamarreño
 * @since 5.0
 */
@Test(groups = "functional", testName = "marshall.ForeignExternalizerTest")
public class ForeignExternalizerTest extends MultipleCacheManagersTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      GlobalConfiguration globalCfg1 = createForeignExternalizerGlobalConfig();
      GlobalConfiguration globalCfg2 = createForeignExternalizerGlobalConfig();
      CacheContainer cm1 = TestCacheManagerFactory.createCacheManager(globalCfg1);
      CacheContainer cm2 = TestCacheManagerFactory.createCacheManager(globalCfg2);
      registerCacheManager(cm1, cm2);
      Configuration cfg = getDefaultClusteredConfig(Configuration.CacheMode.REPL_SYNC);
      defineConfigurationOnAllManagers(getCacheName(), cfg);
      waitForClusterToForm();
   }

   protected String getCacheName() {
      return "ForeignExternalizers";
   }

   protected GlobalConfiguration createForeignExternalizerGlobalConfig() {
      GlobalConfiguration globalCfg = GlobalConfiguration.getClusteredDefault();
      List<ExternalizerConfig> list = new ArrayList<ExternalizerConfig>();
      ExternalizersType type = new ExternalizersType();

      ExternalizerConfig externalizer = new ExternalizerConfig();
      externalizer.setId(1234);
      externalizer.setExternalizerClass("org.infinispan.marshall.ForeignExternalizerTest$IdViaConfigObj$Externalizer");
      list.add(externalizer);

      externalizer = new ExternalizerConfig();
      externalizer.setExternalizerClass("org.infinispan.marshall.ForeignExternalizerTest$IdViaAnnotationObj$Externalizer");
      list.add(externalizer);

      externalizer = new ExternalizerConfig();
      externalizer.setId(3456);
      externalizer.setExternalizerClass("org.infinispan.marshall.ForeignExternalizerTest$IdViaBothObj$Externalizer");
      list.add(externalizer);

      type.setExternalizerConfigs(list);
      globalCfg.setExternalizersType(type);
      return globalCfg;
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

      @Marshalls(typeClasses = IdViaConfigObj.class)
      public static class Externalizer implements org.infinispan.marshall.Externalizer<IdViaConfigObj> {
         @Override
         public void writeObject(ObjectOutput output, IdViaConfigObj object) throws IOException {
            output.writeUTF(object.name);
         }

         @Override
         public IdViaConfigObj readObject(ObjectInput input) throws IOException, ClassNotFoundException {
            return new IdViaConfigObj().setName(input.readUTF());
         }
      }
   }

   public static class IdViaAnnotationObj {
      Date date;

      public IdViaAnnotationObj setDate(Date date) {
         this.date = date;
         return this;
      }

      @Marshalls(typeClasses = IdViaAnnotationObj.class, id = 5678)
      public static class Externalizer implements org.infinispan.marshall.Externalizer<IdViaAnnotationObj> {
         @Override
         public void writeObject(ObjectOutput output, IdViaAnnotationObj object) throws IOException {
            output.writeObject(object.date);
         }

         @Override
         public IdViaAnnotationObj readObject(ObjectInput input) throws IOException, ClassNotFoundException {
            return new IdViaAnnotationObj().setDate((Date) input.readObject());
         }
      }
   }

   public static class IdViaBothObj {
      int age;

      public IdViaBothObj setAge(int age) {
         this.age = age;
         return this;
      }

      @Marshalls(typeClasses = IdViaBothObj.class, id = 9012)
      public static class Externalizer implements org.infinispan.marshall.Externalizer<IdViaBothObj> {
         @Override
         public void writeObject(ObjectOutput output, IdViaBothObj object) throws IOException {
            output.writeInt(object.age);
         }

         @Override
         public IdViaBothObj readObject(ObjectInput input) throws IOException, ClassNotFoundException {
            return new IdViaBothObj().setAge(input.readInt());
         }
      }
   }

}

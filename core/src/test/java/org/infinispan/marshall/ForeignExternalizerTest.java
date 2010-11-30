package org.infinispan.marshall;

import org.infinispan.Cache;
import org.infinispan.CacheException;
import org.infinispan.commands.RemoteCommandsFactory;
import org.infinispan.config.Configuration;
import org.infinispan.config.GlobalConfiguration;
import org.infinispan.config.GlobalConfiguration.MarshallablesType;
import org.infinispan.config.MarshallableConfig;
import org.infinispan.manager.CacheContainer;
import org.infinispan.marshall.jboss.JBossMarshaller;
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
 * // TODO: Document this
 *
 * @author Galder Zamarreño
 * @since // TODO
 */
@Test(groups = "functional", testName = "marshall.ForeignExternalizerTest")
public class ForeignExternalizerTest extends MultipleCacheManagersTest {

   private String cacheName = "ForeignExternalizers";

   @Override
   protected void createCacheManagers() throws Throwable {
      int id = 10;
      GlobalConfiguration globalCfg1 = createForeignExternalizerGlobalConfig(id);
      GlobalConfiguration globalCfg2 = createForeignExternalizerGlobalConfig(id);
      CacheContainer cm1 = TestCacheManagerFactory.createCacheManager(globalCfg1);
      CacheContainer cm2 = TestCacheManagerFactory.createCacheManager(globalCfg2);
      registerCacheManager(cm1, cm2);
      Configuration cfg = getDefaultClusteredConfig(Configuration.CacheMode.REPL_SYNC);
      defineConfigurationOnAllManagers(cacheName, cfg);
      waitForClusterToForm();
   }

   private GlobalConfiguration createForeignExternalizerGlobalConfig(int id) {
      return createForeignExternalizerGlobalConfig(id, false);
   }

   private GlobalConfiguration createForeignExternalizerGlobalConfig(int id, boolean repeat) {
      GlobalConfiguration globalCfg = GlobalConfiguration.getClusteredDefault();
      List<MarshallableConfig> list = new ArrayList<MarshallableConfig>();
      MarshallablesType marshallables = new MarshallablesType();
      int max = 1;
      if (repeat) max = 2;

      for (int i = 0; i < max; i++) {
         addMarshallable(id, list);
      }

      marshallables.setMarshallableConfigs(list);
      globalCfg.setMarshallablesType(marshallables);
      return globalCfg;
   }

   private void addMarshallable(int id, List<MarshallableConfig> list) {
      MarshallableConfig marshallable = new MarshallableConfig();
      marshallable.setId(id);
      marshallable.setExternalizerClass("org.infinispan.marshall.ForeignExternalizerTest$LegacyObjExternalizer");
      marshallable.setTypeClass("org.infinispan.marshall.ForeignExternalizerTest$LegacyObj");
      list.add(marshallable);
   }

   public void testReplicateLegacyPojoWithUserDefinedExternalizer(Method m) {
      Cache cache1 = manager(0).getCache(cacheName);
      Cache cache2 = manager(1).getCache(cacheName);
      LegacyObj legacyObj = new LegacyObj();
      legacyObj.age = 50;
      legacyObj.name = "Galder";
      legacyObj.date = new Date(System.currentTimeMillis());
      cache1.put(m.getName(), legacyObj);
      assert legacyObj.equals(cache2.get(m.getName()));
   }

   public void testForeignExternalizerIdNegative() {
      withExpectedFailure(createForeignExternalizerGlobalConfig(-1),
                          "Should have thrown a CacheException reporting that negative ids are not allowed");
   }

   public void testForeignExternalizerIdClash() {
      withExpectedFailure(createForeignExternalizerGlobalConfig(20, true),
                          "Should have thrown a CacheException reporting duplicate id");
   }

   private void withExpectedFailure(GlobalConfiguration globalCfg, String message) {
      JBossMarshaller jbmarshaller = new JBossMarshaller();
      ClassLoader cl = Thread.currentThread().getContextClassLoader();
      try {
         jbmarshaller.start(cl, new RemoteCommandsFactory(), null, globalCfg);
         assert false : message;
      } catch (CacheException ce) {
         log.trace("Expected exception", ce);
      } finally {
         jbmarshaller.stop();
      }
   }

   public static class LegacyObj {
      byte age;
      String name;
      Date date;

      @Override
      public boolean equals(Object o) {
         if (this == o) return true;
         if (o == null || getClass() != o.getClass()) return false;

         LegacyObj legacyObj = (LegacyObj) o;

         if (age != legacyObj.age) return false;
         if (date != null ? !date.equals(legacyObj.date) : legacyObj.date != null) return false;
         if (name != null ? !name.equals(legacyObj.name) : legacyObj.name != null) return false;

         return true;
      }

      @Override
      public int hashCode() {
         int result = (int) age;
         result = 31 * result + (name != null ? name.hashCode() : 0);
         result = 31 * result + (date != null ? date.hashCode() : 0);
         return result;
      }
   }

   public static class LegacyObjExternalizer implements Externalizer {
      @Override
      public void writeObject(ObjectOutput output, Object object) throws IOException {
         LegacyObj legacyObj = (LegacyObj) object;
         output.write(legacyObj.age);
         output.writeUTF(legacyObj.name);
         output.writeObject(legacyObj.date);
      }

      @Override
      public Object readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         LegacyObj legacyObj = new LegacyObj();
         legacyObj.age = (byte) input.read();
         legacyObj.name = input.readUTF();
         legacyObj.date = (Date) input.readObject();
         return legacyObj;
      }
   }

}

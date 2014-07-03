package org.infinispan.query.externalizers;

import org.apache.lucene.index.Term;
import org.apache.lucene.util.BytesRef;
import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.CacheContainer;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import static org.junit.Assert.assertEquals;

@Test(groups = "functional", testName = "query.externalizers.LuceneTerm")
public class TermExternalizerTest extends MultipleCacheManagersTest {

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
      return "QueryExternalizers-LuceneTerm";
   }

   protected GlobalConfigurationBuilder createForeignExternalizerGlobalConfig() {
      //Needed Externalizers should be picked up automatically via the Module system
      return new GlobalConfigurationBuilder().clusteredDefault();
   }

   public void emptyPayloadTest() {
      BytesRef payload = new BytesRef();
      Term t = new Term("hello terms world!", payload);
      assertMarshallable(t);
   }

   public void somePayloadTest() {
      BytesRef payload = new BytesRef(new byte[]{ 0, 7, 3});
      Term t = new Term("hello terms world!", payload);
      assertMarshallable(t);
   }

   public void offsetPayloadTest() {
      BytesRef payload = new BytesRef(new byte[]{ 0, 7, 3}, 1, 2);
      Term t = new Term("hello terms world!", payload);
      assertMarshallable(t);
   }

   public void offsetLimitedPayloadTest() {
      BytesRef payload = new BytesRef(new byte[]{ 0, 7, 3, 2, 2, 7}, 1, 2);
      Term t = new Term("hello terms world!", payload);
      assertMarshallable(t);
   }

   private void assertMarshallable(final Term obj) {
      Cache cache1 = manager(0).getCache(getCacheName());
      Cache cache2 = manager(1).getCache(getCacheName());
      cache1.put("key", obj);
      final Term beamedUpObject = (Term) cache2.get("key");
      assertEquals(obj, beamedUpObject);
      assertEquals(obj.field(), beamedUpObject.field());
      assertEquals(obj.text(), beamedUpObject.text());
      final BytesRef referencePayload = obj.bytes();
      Object clonedPayload = beamedUpObject.bytes();
      assertEquals(referencePayload, clonedPayload);
      assertEquals(obj.toString(), beamedUpObject.toString());
   }

}

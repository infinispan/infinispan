package org.infinispan.api;

import static org.infinispan.test.TestingUtil.k;
import static org.infinispan.test.TestingUtil.v;
import static org.testng.AssertJUnit.assertEquals;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.testing.Exceptions;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "api.StaticCacheAliasTest")
public class StaticCacheAliasTest extends SingleCacheManagerTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      GlobalConfigurationBuilder globalBuilder = new GlobalConfigurationBuilder().nonClusteredDefault();
      EmbeddedCacheManager cm = TestCacheManagerFactory.fromString("""
            <infinispan>
            <local-cache name="cacheA" aliases="0"/>
            <local-cache name="cacheB" aliases="1"/>
            </infinispan>
            """);
      return cm;
   }

   public void testCacheAliases() {
      cacheManager.getCache("cacheA").put(k(), v());
      cacheManager.getCache("cacheB").put(k(), v(1));
      assertEquals(v(), cacheManager.getCache("0").get(k()));
      assertEquals(v(1), cacheManager.getCache("1").get(k()));
   }

   public void testAliasConflict() {
      Exceptions.expectException(CacheConfigurationException.class, "^ISPN000702:.*", () ->
            cacheManager.defineConfiguration("cacheC", new ConfigurationBuilder().aliases("0").build())
      );
   }


}

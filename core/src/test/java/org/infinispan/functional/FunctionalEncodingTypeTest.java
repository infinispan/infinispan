package org.infinispan.functional;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.dataconversion.IdentityEncoder;
import org.infinispan.commons.dataconversion.JavaSerializationEncoder;
import org.infinispan.functional.impl.FunctionalMapImpl;
import org.infinispan.manager.EmbeddedCacheManager;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "functional.FunctionalEncoderTest")
public class FunctionalEncodingTypeTest extends FunctionalMapTest {

   @Override
   protected void initMaps() {
      AdvancedCache advancedCacheL1 = cacheManagers.get(0).<Integer, String>getCache().getAdvancedCache()
            .withEncoding(IdentityEncoder.class, JavaSerializationEncoder.class);
      AdvancedCache advancedCacheL2 = cacheManagers.get(0).<Integer, String>getCache().getAdvancedCache()
            .withEncoding(IdentityEncoder.class, JavaSerializationEncoder.class);
      AdvancedCache advancedCacheDist1 = cacheManagers.get(0).<Integer, String>getCache(DIST).getAdvancedCache()
            .withEncoding(IdentityEncoder.class, JavaSerializationEncoder.class);
      AdvancedCache advancedCacheDist2 = cacheManagers.get(1).<Integer, String>getCache(DIST).getAdvancedCache()
            .withEncoding(IdentityEncoder.class, JavaSerializationEncoder.class);
      AdvancedCache advancedCacheRep1 = cacheManagers.get(0).<Integer, String>getCache(REPL).getAdvancedCache()
            .withEncoding(IdentityEncoder.class, JavaSerializationEncoder.class);
      AdvancedCache advancedCacheRep2 = cacheManagers.get(1).<Integer, String>getCache(REPL).getAdvancedCache()
            .withEncoding(IdentityEncoder.class, JavaSerializationEncoder.class);
      fmapL1 = FunctionalMapImpl.create(advancedCacheL1);
      fmapL2 = FunctionalMapImpl.create(advancedCacheL2);
      fmapD1 = FunctionalMapImpl.create(advancedCacheDist1);
      fmapD2 = FunctionalMapImpl.create(advancedCacheDist2);
      fmapR1 = FunctionalMapImpl.create(advancedCacheRep1);
      fmapR2 = FunctionalMapImpl.create(advancedCacheRep2);
   }

   @Override
   protected AdvancedCache<?, ?> getAdvancedCache(EmbeddedCacheManager cm, String cacheName) {
      return cm.getCache(cacheName).getAdvancedCache().withEncoding(IdentityEncoder.class, JavaSerializationEncoder.class);
   }
}

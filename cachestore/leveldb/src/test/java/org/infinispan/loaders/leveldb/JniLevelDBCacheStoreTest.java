package org.infinispan.loaders.leveldb;

import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.loaders.leveldb.LevelDBCacheStoreConfig.ImplementationType;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "loaders.leveldb.JniLevelDBCacheStoreTest")
public class JniLevelDBCacheStoreTest extends LevelDBCacheStoreTest {

	protected LevelDBCacheStoreConfig createCacheStoreConfig()
         throws CacheLoaderException {
      LevelDBCacheStoreConfig cfg = super.createCacheStoreConfig();
      cfg.setImplementationType(ImplementationType.JNI.toString());
      return cfg;
   }
}

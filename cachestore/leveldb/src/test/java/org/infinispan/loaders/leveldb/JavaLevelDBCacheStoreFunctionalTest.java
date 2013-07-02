package org.infinispan.loaders.leveldb;

import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.loaders.CacheStoreConfig;
import org.infinispan.loaders.leveldb.LevelDBCacheStoreConfig.ImplementationType;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "loaders.leveldb.JavaLevelDBCacheStoreFunctionalTest")
public class JavaLevelDBCacheStoreFunctionalTest extends
		LevelDBCacheStoreFunctionalTest {

	@Override
	protected CacheStoreConfig createCacheStoreConfig()
			throws CacheLoaderException {
		LevelDBCacheStoreConfig cfg = (LevelDBCacheStoreConfig) super.createCacheStoreConfig();
		cfg.setImplementationType(ImplementationType.JAVA.toString());
		return cfg;
	}
}

package org.infinispan.loaders.leveldb;

import java.io.File;

import org.infinispan.loaders.BaseCacheStoreFunctionalTest;
import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.loaders.CacheStoreConfig;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "loaders.leveldb.LevelDBCacheStoreFunctionalTest")
public class LevelDBCacheStoreFunctionalTest extends
		BaseCacheStoreFunctionalTest {
	private String tmpDirectory;

	@BeforeClass
	protected void setUpTempDir() {
		tmpDirectory = TestingUtil.tmpDirectory(this);
	}

	@AfterClass(alwaysRun = true)
	protected void clearTempDir() {
		TestingUtil.recursiveFileRemove(tmpDirectory);
		new File(tmpDirectory).mkdirs();
	}

	@Override
	protected CacheStoreConfig createCacheStoreConfig()
			throws CacheLoaderException {
		LevelDBCacheStoreConfig cfg = new LevelDBCacheStoreConfig();
		cfg.setLocation(tmpDirectory + "/data");
		cfg.setExpiredLocation(tmpDirectory + "/expiry");
		cfg.setClearThreshold(2);
		cfg.setPurgeSynchronously(true); // for more accurate unit testing
		return cfg;
	}
}

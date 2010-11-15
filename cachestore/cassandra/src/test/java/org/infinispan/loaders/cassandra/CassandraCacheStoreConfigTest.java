package org.infinispan.loaders.cassandra;

import org.infinispan.loaders.CacheLoaderException;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "loaders.cassandra.CassandraCacheStoreConfigTest")
public class CassandraCacheStoreConfigTest {

	
	public void setConfigurationPropertiesFileTest() throws CacheLoaderException {
		CassandraCacheStoreConfig config = new CassandraCacheStoreConfig();
		config.setConfigurationPropertiesFile("cassandrapool.properties");
		
		assert config.poolProperties.getInitialSize()==2;
		assert config.poolProperties.getSocketTimeout()==10000;
		assert config.poolProperties.isTestOnBorrow();
		assert !config.poolProperties.isTestOnReturn();
		assert config.poolProperties.isTestWhileIdle();
	}
}

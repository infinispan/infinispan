package org.infinispan.loaders.cassandra;

import java.io.IOException;

import org.apache.cassandra.config.ConfigurationException;
import org.apache.thrift.transport.TTransportException;
import org.infinispan.loaders.BaseCacheStoreTest;
import org.infinispan.loaders.CacheStore;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "loaders.cassandra.CassandraCacheStoreTest")
public class CassandraCacheStoreTest extends BaseCacheStoreTest {
	private static EmbeddedServerHelper embedded;

	/**
	 * Set embedded cassandra up and spawn it in a new thread.
	 * 
	 * @throws TTransportException
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws ConfigurationException
	 */
	@BeforeClass
	public static void setup() throws TTransportException, IOException, InterruptedException, ConfigurationException {
		embedded = new EmbeddedServerHelper();
		embedded.setup();
	}
	
	@AfterClass
	public static void cleanup() {
		EmbeddedServerHelper.teardown();
		embedded = null;
	}

	@Override
	protected CacheStore createCacheStore() throws Exception {
		CassandraCacheStore cs = new CassandraCacheStore();
		CassandraCacheStoreConfig clc = new CassandraCacheStoreConfig();
		clc.setHost("localhost");
		clc.setKeySpace("Infinispan");
		cs.init(clc, getCache(), getMarshaller());
		cs.start();
		return cs;
	}

}

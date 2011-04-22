/*
 * JBoss, Home of Professional Open Source
 * Copyright 2010 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan.loaders.cassandra;

import java.io.File;
import java.io.IOException;
import java.net.URL;

import org.apache.cassandra.service.EmbeddedCassandraService;
import org.apache.thrift.transport.TTransportException;
import org.infinispan.loaders.BaseCacheStoreTest;
import org.infinispan.loaders.CacheStore;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "loaders.cassandra.CassandraCacheStoreTest")
public class CassandraCacheStoreTest extends BaseCacheStoreTest {
	private static EmbeddedCassandraService cassandra;

	/**
	 * Set embedded cassandra up and spawn it in a new thread.
	 * 
	 * @throws TTransportException
	 * @throws IOException
	 * @throws InterruptedException
	 */
	@BeforeClass
	public static void setup() throws TTransportException, IOException, InterruptedException {
		// Tell cassandra where the configuration files are.
		// Use the test configuration file.
		URL resource = Thread.currentThread().getContextClassLoader().getResource("storage-conf.xml");
		String configPath = resource.getPath().substring(0, resource.getPath().lastIndexOf(File.separatorChar));
		
		System.setProperty("storage-config", configPath);

		CassandraServiceDataCleaner cleaner = new CassandraServiceDataCleaner();
		cleaner.prepare();
		cassandra = new EmbeddedCassandraService();
		cassandra.init();
		Thread t = new Thread(cassandra);
		t.setDaemon(true);
		t.start();
	}
	
	@AfterClass
	public static void cleanup() {
		System.exit(0);
	}

	@Override
	protected CacheStore createCacheStore() throws Exception {
		CassandraCacheStore cs = new CassandraCacheStore();
		CassandraCacheStoreConfig clc = new CassandraCacheStoreConfig();
		clc.setHost("localhost");
		cs.init(clc, getCache(), getMarshaller());
		cs.start();
		return cs;
	}

}

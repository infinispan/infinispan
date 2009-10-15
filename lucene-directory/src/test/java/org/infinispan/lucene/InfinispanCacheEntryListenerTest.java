/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2009, Red Hat Middleware LLC, and individual contributors
 * as indicated by the @author tags. See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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
package org.infinispan.lucene;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.infinispan.Cache;
import org.infinispan.manager.CacheManager;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

/**
 * @author Lukasz Moren
 * @author Sanne Grinovero
 */
@Test(groups = "functional", testName = "lucene.InfinispanCacheEntryListenerTest")
public class InfinispanCacheEntryListenerTest {

	Cache<CacheKey, Object> cache;
	CacheManager cm;

	public void testCacheEntryCreatedListener() {
		final FileListCacheKey fileListKey = new FileListCacheKey( "index" );

		//add some data
		cache.put( fileListKey, Collections.synchronizedMap( new HashMap<String, String>() ) );
		cache.addListener( new InfinispanCacheEntryListener( "index" ) );

		final FileCacheKey key1 = new FileCacheKey( "index", "Hello.txt" );
		cache.put( key1, new FileMetadata() );
		cache.put( new ChunkCacheKey( "index", "Chunk.txt", 0 ), new byte[15] );
		cache.put( new FileCacheKey( "index", "write.lock", true ), "");

		final FileCacheKey key2 = new FileCacheKey( "index", "World.txt" );
		cache.put( key2, new FileMetadata() );

		final FileCacheKey key3 = new FileCacheKey( "index2", "Other.txt" );
		cache.put( key3, new FileMetadata() );

		//check if cache listener properly added file names to the list
		Map map = ( Map ) cache.get( fileListKey );

		assert map.containsKey( "Hello.txt" );
		assert ! map.containsKey( "Chunk.txt" );
		assert ! map.containsKey( "write.lock" );
		assert map.containsKey( "World.txt" );
		assert ! map.containsKey( "Other.txt" );

		cache.remove( key1 );
		cache.remove( key2 );
		cache.remove( key3 );

		//and properly removed file names from the list
		map = ( Map ) cache.get( fileListKey );

		assert ! map.containsKey( "Hello.txt" );
		assert ! map.containsKey( "World.txt" );
		assert ! map.containsKey( "Other.txt" );
	}

	@BeforeTest
	public void setUp() {
	   cm = CacheTestSupport.createTestCacheManager();
	   cache = cm.getCache();
	}

	@AfterTest
	public void tearDown() {
		if ( cache != null ) {
			cache.stop();
		}
		if ( cm != null ) {
		   cm.stop();
		}
	}
}

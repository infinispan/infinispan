/*
 * Copyright 2011 Red Hat, Inc. and/or its affiliates.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA
 */
package org.infinispan.server.hotrod.util;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.distexec.mapreduce.Collator;
import org.infinispan.distexec.mapreduce.Collector;
import org.infinispan.distexec.mapreduce.MapReduceTask;
import org.infinispan.distexec.mapreduce.Mapper;
import org.infinispan.distexec.mapreduce.Reducer;
import org.infinispan.server.core.CacheValue;
import org.infinispan.util.ByteArrayKey;

/**
 * 
 * @author <a href="mailto:rtsang@redhat.com">Ray Tsang</a>
 * @since 5.2
 *
 */
public final class BulkUtil {
	public static final Set<ByteArrayKey> getAllKeys(Cache<ByteArrayKey, CacheValue> cache) {
		CacheMode cacheMode = cache.getAdvancedCache().getCacheConfiguration().clustering().cacheMode(); 
		if (cacheMode == CacheMode.LOCAL || cacheMode == CacheMode.REPL_ASYNC || cacheMode == CacheMode.REPL_SYNC) {
			return cache.keySet();
		} else {
			MapReduceTask<ByteArrayKey, CacheValue, ByteArrayKey, Object> task = new MapReduceTask<ByteArrayKey, CacheValue, ByteArrayKey, Object>(cache)
					.mappedWith(new KeyMapper())
					.reducedWith(new KeyReducer());
			return task.execute(new KeysCollator());
		}
	}
	
	static class KeyMapper implements Mapper<ByteArrayKey, CacheValue, ByteArrayKey, Object> {
		private static final long serialVersionUID = -5054573988280497412L;

		@Override
		public void map(ByteArrayKey key, CacheValue value,
				Collector<ByteArrayKey, Object> collector) {
			collector.emit(key, null);
		}
	}
	
	static class KeyReducer implements Reducer<ByteArrayKey, Object> {
		private static final long serialVersionUID = -8199097945001793869L;

		@Override
		public Boolean reduce(ByteArrayKey reducedKey, Iterator<Object> iter) {
			return iter.hasNext();
		}
	}
	
	static class KeysCollator implements Collator<ByteArrayKey, Object, Set<ByteArrayKey>> {
		@Override
		public Set<ByteArrayKey> collate(Map<ByteArrayKey, Object> reducedResults) {
			return reducedResults.keySet();
		}
		
	}
}

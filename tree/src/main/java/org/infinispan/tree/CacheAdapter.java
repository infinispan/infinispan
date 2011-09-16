/*
 * JBoss, Home of Professional Open Source
 * Copyright 2011 Red Hat Inc. and/or its affiliates and other
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

package org.infinispan.tree;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.config.Configuration;
import org.infinispan.context.InvocationContextContainer;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.util.concurrent.NotifyingFuture;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * A cache adapter for setting flags for each individual cache operation called
 * within the scope of a TreeCache operation.
 *
 * Note: We can't just wrap around use of cache in tree module because flag
 * assignment needs to extend even to calls within AtomicHashMapProxy.getDeltaMapForWrite().
 * So, the only possible way is to wrap around the Cache instance around
 * something that sets flags accordingly.
 *
 * @author Galder Zamarreño
 * @since 4.2
 */
public class CacheAdapter implements Cache {

   private final Cache cache;
   private final TreeContextContainer tcc;
   private final InvocationContextContainer icc;

   public CacheAdapter(Cache cache,
                       TreeContextContainer tcc,
                       InvocationContextContainer icc) {
      this.cache = cache;
      this.tcc = tcc;
      this.icc = icc;
   }

   @Override
   public void putForExternalRead(Object key, Object value) {
      withFlags().putForExternalRead(key, value);
   }

   @Override
   public void evict(Object key) {
      withFlags().evict(key);
   }

   @Override
   public Configuration getConfiguration() {
      return cache.getConfiguration();
   }

   @Override
   public boolean startBatch() {
      return cache.startBatch();
   }

   @Override
   public void endBatch(boolean successful) {
      cache.endBatch(successful);
   }

   @Override
   public String getName() {
      return cache.getName();
   }

   @Override
   public String getVersion() {
      return cache.getVersion();
   }

   @Override
   public EmbeddedCacheManager getCacheManager() {
      return cache.getCacheManager();
   }

   @Override
   public Object put(Object key, Object value, long lifespan, TimeUnit unit) {
      return withFlags().put(key, value, lifespan, unit);
   }

   @Override
   public Object putIfAbsent(Object key, Object value, long lifespan, TimeUnit unit) {
      return withFlags().putIfAbsent(key, value, lifespan, unit);
   }

   @Override
   public void putAll(Map map, long lifespan, TimeUnit unit) {
      withFlags().putAll(map, lifespan, unit);
   }

   @Override
   public Object replace(Object key, Object value, long lifespan, TimeUnit unit) {
      return withFlags().replace(key, value, lifespan, unit);
   }

   @Override
   public boolean replace(Object key, Object oldValue, Object value, long lifespan, TimeUnit unit) {
      return withFlags().replace(key, oldValue, value, lifespan, unit);
   }

   @Override
   public Object put(Object key, Object value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      return withFlags().put(key, value, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit);
   }

   @Override
   public Object putIfAbsent(Object key, Object value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      return withFlags().putIfAbsent(key, value, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit);
   }

   @Override
   public void putAll(Map map, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      withFlags().putAll(map, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit);
   }

   @Override
   public Object replace(Object key, Object value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      return withFlags().replace(key, value, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit);
   }

   @Override
   public boolean replace(Object key, Object oldValue, Object value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      return withFlags().replace(key, oldValue, value, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit);
   }

   @Override
   public NotifyingFuture putAsync(Object key, Object value) {
      return withFlags().putAsync(key, value);
   }

   @Override
   public NotifyingFuture putAsync(Object key, Object value, long lifespan, TimeUnit unit) {
      return withFlags().putAsync(key, value, lifespan, unit);
   }

   @Override
   public NotifyingFuture putAsync(Object key, Object value, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      return withFlags().putAsync(key, value, lifespan, lifespanUnit, maxIdle, maxIdleUnit);
   }

   @Override
   public NotifyingFuture<Void> putAllAsync(Map data) {
      return withFlags().putAllAsync(data);
   }

   @Override
   public NotifyingFuture<Void> putAllAsync(Map data, long lifespan, TimeUnit unit) {
      return withFlags().putAllAsync(data, lifespan, unit);
   }

   @Override
   public NotifyingFuture<Void> putAllAsync(Map data, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      return withFlags().putAllAsync(data, lifespan, lifespanUnit, maxIdle, maxIdleUnit);
   }

   @Override
   public NotifyingFuture<Void> clearAsync() {
      return withFlags().clearAsync();
   }

   @Override
   public NotifyingFuture putIfAbsentAsync(Object key, Object value) {
      return withFlags().putIfAbsentAsync(key, value);
   }

   @Override
   public NotifyingFuture putIfAbsentAsync(Object key, Object value, long lifespan, TimeUnit unit) {
      return withFlags().putIfAbsentAsync(key, value, lifespan, unit);
   }

   @Override
   public NotifyingFuture putIfAbsentAsync(Object key, Object value, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      return withFlags().putIfAbsentAsync(key, value, lifespan, lifespanUnit, maxIdle, maxIdleUnit);
   }

   @Override
   public NotifyingFuture removeAsync(Object key) {
      return withFlags().removeAsync(key);
   }

   @Override
   public NotifyingFuture<Boolean> removeAsync(Object key, Object value) {
      return withFlags().removeAsync(key, value);
   }

   @Override
   public NotifyingFuture replaceAsync(Object key, Object value) {
      return withFlags().replaceAsync(key, value);
   }

   @Override
   public NotifyingFuture replaceAsync(Object key, Object value, long lifespan, TimeUnit unit) {
      return withFlags().replaceAsync(key, value, lifespan, unit);
   }

   @Override
   public NotifyingFuture replaceAsync(Object key, Object value, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      return withFlags().replaceAsync(key, value, lifespan, lifespanUnit, maxIdle, maxIdleUnit);
   }

   @Override
   public NotifyingFuture<Boolean> replaceAsync(Object key, Object oldValue, Object newValue) {
      return withFlags().replaceAsync(key, oldValue, newValue);
   }

   @Override
   public NotifyingFuture<Boolean> replaceAsync(Object key, Object oldValue, Object newValue, long lifespan, TimeUnit unit) {
      return withFlags().replaceAsync(key, oldValue, newValue, lifespan, unit);
   }

   @Override
   public NotifyingFuture<Boolean> replaceAsync(Object key, Object oldValue, Object newValue, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      return withFlags().replaceAsync(key, oldValue, newValue, lifespan, lifespanUnit, maxIdle, maxIdleUnit);
   }

   @Override
   public AdvancedCache getAdvancedCache() {
      return cache.getAdvancedCache();
   }

   @Override
   public void compact() {
      withFlags().compact();
   }

   @Override
   public ComponentStatus getStatus() {
      return cache.getStatus();
   }

   @Override
   public int size() {
      return withFlags().size();
   }

   @Override
   public boolean isEmpty() {
      return withFlags().isEmpty();
   }

   @Override
   public boolean containsKey(Object key) {
      return withFlags().containsKey(key);
   }

   @Override
   public boolean containsValue(Object value) {
      return withFlags().containsValue(value);
   }

   @Override
   public Object get(Object key) {
      return withFlags().get(key);
   }

   @Override
   public Object put(Object key, Object value) {
      return withFlags().put(key, value);
   }

   @Override
   public Object remove(Object key) {
      return withFlags().remove(key);
   }

   @Override
   public void putAll(Map m) {
      withFlags().putAll(m);
   }

   @Override
   public void clear() {
      withFlags().clear();
   }

   @Override
   public Set keySet() {
      return withFlags().keySet();
   }

   @Override
   public Collection values() {
      return withFlags().values();
   }

   @Override
   public Set entrySet() {
      return withFlags().entrySet();
   }

   @Override
   public Object putIfAbsent(Object key, Object value) {
      return withFlags().putIfAbsent(key, value);
   }

   @Override
   public boolean remove(Object key, Object value) {
      return withFlags().remove(key, value);
   }

   @Override
   public boolean replace(Object key, Object oldValue, Object newValue) {
      return withFlags().replace(key, oldValue, newValue);
   }

   @Override
   public Object replace(Object key, Object value) {
      return withFlags().replace(key, value);
   }

   @Override
   public void start() {
      cache.start();
   }

   @Override
   public void stop() {
      cache.stop();
   }

   @Override
   public void addListener(Object listener) {
      cache.addListener(listener);
   }

   @Override
   public void removeListener(Object listener) {
      cache.removeListener(listener);
   }

   @Override
   public Set<Object> getListeners() {
      return cache.getListeners();
   }

   @Override
   public NotifyingFuture getAsync(Object key) {
      return cache.getAsync(key);
   }

   private Cache withFlags() {
      if (tcc.getTreeContext() != null)
         icc.createInvocationContext(true).setFlags(tcc.getTreeContext().getFlags());
      return cache;
   }
}

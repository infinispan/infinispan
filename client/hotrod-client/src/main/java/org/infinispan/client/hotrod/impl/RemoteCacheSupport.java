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
package org.infinispan.client.hotrod.impl;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.commons.util.concurrent.NotifyingFuture;

/**
 * Purpose: keep all delegating and unsupported methods in one place -> readability.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
public abstract class RemoteCacheSupport<K,V> implements RemoteCache<K,V> {
   protected long defaultLifespan;
   protected long defaultMaxIdleTime;

   protected RemoteCacheSupport() {
      this(0, 0);
   }

   protected RemoteCacheSupport(long defaultLifespan, long defaultMaxIdleTime) {
      this.defaultLifespan = defaultLifespan;
      this.defaultMaxIdleTime = defaultMaxIdleTime;
   }

   @Override
   public void putAll(Map<? extends K, ? extends V> map, long lifespan, TimeUnit unit) {
      putAll(map, lifespan, unit, defaultMaxIdleTime, MILLISECONDS);
   }

   @Override
   public NotifyingFuture<Void> putAllAsync(Map<? extends K, ? extends V> data) {
      return putAllAsync(data, defaultLifespan, MILLISECONDS, defaultMaxIdleTime, MILLISECONDS);
   }

   @Override
   public NotifyingFuture<Void> putAllAsync(Map<? extends K, ? extends V> data, long lifespan, TimeUnit unit) {
      return putAllAsync(data, lifespan, unit, defaultMaxIdleTime, MILLISECONDS);
   }

   @Override
   public void putAll(Map<? extends K, ? extends V> map) {
      putAll(map, defaultLifespan, MILLISECONDS, defaultMaxIdleTime, MILLISECONDS);
   }

   @Override
   public V putIfAbsent(K key, V value) {
      return putIfAbsent(key, value, defaultLifespan, MILLISECONDS, defaultMaxIdleTime, MILLISECONDS);
   }

   @Override
   public boolean replace(K key, V oldValue, V newValue) {
      throw new UnsupportedOperationException();
   }

   @Override
   public V replace(K key, V value) {
      return replace(key, value, defaultLifespan, MILLISECONDS, defaultMaxIdleTime, MILLISECONDS);
   }

   @Override
   public V put(K key, V value) {
      return put(key, value, defaultLifespan, MILLISECONDS, defaultMaxIdleTime, MILLISECONDS);
   };

   /**
    * This is intentionally a non-public method meant as an integration point for bytecode manipulation. Don't remove or
    * alter the signature even if it might look like unreachable code. Implementors should perform a put operation but
    * optimizing it as return values are not required.
    *
    * @author Sanne Grinovero <sanne@infinispan.org> (C) 2011 Red Hat Inc.
    * @since 5.0
    */
   protected abstract void set(K key, V value);

   @Override
   public final NotifyingFuture<V> putAsync(K key, V value) {
      return putAsync(key, value, defaultLifespan, MILLISECONDS, defaultMaxIdleTime, MILLISECONDS);
   }

   @Override
   public NotifyingFuture<Boolean> replaceWithVersionAsync(K key, V newValue, long version) {
      return replaceWithVersionAsync(key, newValue, version, 0);
   }

   @Override
   public NotifyingFuture<Boolean> replaceWithVersionAsync(K key, V newValue, long version, int lifespanSeconds) {
      return replaceWithVersionAsync(key, newValue, version, lifespanSeconds, 0);
   }

   @Override
   public boolean replaceWithVersion(K key, V newValue, long version) {
      return replaceWithVersion(key, newValue, version, 0);
   }

   @Override
   public boolean replaceWithVersion(K key, V newValue, long version, int lifespanSeconds) {
      return replaceWithVersion(key, newValue, version, lifespanSeconds, 0);
   }

   @Override
   public NotifyingFuture<V> replaceAsync(K key, V value) {
      return replaceAsync(key, value, defaultLifespan, MILLISECONDS, defaultMaxIdleTime, MILLISECONDS);
   }

   @Override
   public V put(K key, V value, long lifespan, TimeUnit unit) {
      return put(key, value, lifespan, unit, defaultMaxIdleTime, MILLISECONDS);
   }


   @Override
   public NotifyingFuture<V> putIfAbsentAsync(K key, V value) {
      return putIfAbsentAsync(key, value, defaultLifespan, MILLISECONDS, defaultMaxIdleTime, MILLISECONDS);
   }

   @Override
   public NotifyingFuture<V> putIfAbsentAsync(K key, V value, long lifespan, TimeUnit lifespanUnit) {
      return putIfAbsentAsync(key, value, lifespan, lifespanUnit, defaultMaxIdleTime, MILLISECONDS);
   }

   @Override
   public boolean containsValue(Object value) {
      throw new UnsupportedOperationException();
   }

   @Override
   public Collection<V> values() {
      throw new UnsupportedOperationException();
   }

   @Override
   public Set<Entry<K, V>> entrySet() {
      throw new UnsupportedOperationException();
   }

   @Override
   public boolean remove(Object key, Object value) {
      throw new UnsupportedOperationException();
   }
}

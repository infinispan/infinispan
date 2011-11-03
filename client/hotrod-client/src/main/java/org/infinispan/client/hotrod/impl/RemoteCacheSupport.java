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

import org.infinispan.CacheSupport;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.util.concurrent.NotifyingFuture;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Purpose: keep all delegating and unsupported methods in one place -> readability.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
public abstract class RemoteCacheSupport<K,V> extends CacheSupport<K,V> implements RemoteCache<K,V> {


   @Override
   public NotifyingFuture<Boolean> replaceWithVersionAsync(K key, V newValue, long version) {
      return replaceWithVersionAsync(key, newValue, version, 0);
   }

   @Override
   public NotifyingFuture<Boolean> replaceWithVersionAsync(K key, V newValue, long version, int lifespanSeconds) {
      return replaceWithVersionAsync(key, newValue, version, 0, 0);
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
   public boolean containsValue(Object value) {
      throw new UnsupportedOperationException();
   }

   @Override
   public Set<K> keySet() {
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

   @Override
   public NotifyingFuture<Boolean> removeAsync(Object key, Object value) {
      throw new UnsupportedOperationException();
   }

   @Override
   public boolean replace(K key, V oldValue, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      throw new UnsupportedOperationException();
   }


   @Override
   public NotifyingFuture<Boolean> replaceAsync(K key, V oldValue, V newValue, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      throw new UnsupportedOperationException();
   }
}

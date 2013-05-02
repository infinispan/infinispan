/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009 Red Hat Inc. and/or its affiliates and other
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
package org.infinispan.test.fwk;

import org.infinispan.metadata.EmbeddedMetadata;
import org.infinispan.container.InternalEntryFactory;
import org.infinispan.container.InternalEntryFactoryImpl;
import org.infinispan.container.entries.ImmortalCacheEntry;
import org.infinispan.container.entries.ImmortalCacheValue;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.InternalCacheValue;
import org.infinispan.container.entries.MortalCacheValue;
import org.infinispan.container.entries.TransientCacheValue;
import org.infinispan.container.entries.TransientMortalCacheValue;

import static org.infinispan.test.AbstractInfinispanTest.TIME_SERVICE;

/**
 * A factory for internal entries for the test suite
 */
public class TestInternalCacheEntryFactory {

   private static final InternalEntryFactory FACTORY = new InternalEntryFactoryImpl();

   static {
      ((InternalEntryFactoryImpl) FACTORY).injectTimeService(TIME_SERVICE);
   }

   public static InternalCacheEntry create(Object key, Object value) {
      return new ImmortalCacheEntry(key, value);
   }

   //
   public static InternalCacheEntry create(Object key, Object value, long lifespan) {
      return FACTORY.create(key, value, null, lifespan, -1);
   }

   public static InternalCacheEntry create(Object key, Object value, long lifespan, long maxIdle) {
      return FACTORY.create(key, value, null, lifespan, maxIdle);
   }

   public static InternalCacheEntry create(Object key, Object value, long created, long lifespan, long lastUsed, long maxIdle) {
      return FACTORY.create(key, value, new EmbeddedMetadata.Builder().build(),
            created, lifespan, lastUsed, maxIdle);
   }

   public static InternalCacheValue createValue(Object v, long created, long lifespan, long lastUsed, long maxIdle) {
      if (lifespan < 0 && maxIdle < 0) return new ImmortalCacheValue(v);
      if (lifespan > -1 && maxIdle < 0) return new MortalCacheValue(v, created, lifespan);
      if (lifespan < 0 && maxIdle > -1) return new TransientCacheValue(v, maxIdle, lastUsed);
      return new TransientMortalCacheValue(v, created, lifespan, maxIdle, lastUsed);
   }

}

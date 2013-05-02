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
package org.infinispan.eviction;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.test.AbstractInfinispanTest;
import org.testng.annotations.Test;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

@Test(groups = "unit", testName = "eviction.EvictionManagerTest")
public class EvictionManagerTest extends AbstractInfinispanTest {

   private ConfigurationBuilder getCfg() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.eviction().strategy(EvictionStrategy.LRU).maxEntries(123);
      return builder;
   }

   public void testNoEvictionThread() {
      EvictionManagerImpl em = new EvictionManagerImpl();
      Configuration cfg = getCfg().expiration().wakeUpInterval(0L).build();

      ScheduledExecutorService mockService = mock(ScheduledExecutorService.class);
      em.initialize(mockService, "", cfg, null, null, null, null);
      em.start();

      assert em.evictionTask == null : "Eviction task is not null!  Should not have scheduled anything!";
   }

   public void testWakeupInterval() {
      EvictionManagerImpl em = new EvictionManagerImpl();
      Configuration cfg = getCfg().expiration().wakeUpInterval(789L).build();

      ScheduledExecutorService mockService = mock(ScheduledExecutorService.class);
      em.initialize(mockService, "", cfg, null, null, null, null);

      ScheduledFuture mockFuture = mock(ScheduledFuture.class);
      when(mockService.scheduleWithFixedDelay(isA(EvictionManagerImpl.ScheduledTask.class), eq(789l),
                                                eq(789l), eq(TimeUnit.MILLISECONDS)))
            .thenReturn(mockFuture);
      em.start();

      assert em.evictionTask == mockFuture;
      verify(mockService).scheduleWithFixedDelay(any(Runnable.class), anyLong(), anyLong(), any(TimeUnit.class)); // expect that the executor was never used!!
   }
}

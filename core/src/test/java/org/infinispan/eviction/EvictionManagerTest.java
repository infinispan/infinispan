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

import org.infinispan.config.Configuration;
import org.infinispan.test.AbstractInfinispanTest;
import org.testng.annotations.Test;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static org.easymock.EasyMock.*;

@Test(groups = "unit", testName = "eviction.EvictionManagerTest")
public class EvictionManagerTest extends AbstractInfinispanTest {

   private Configuration getCfg() {
      return new Configuration().fluent()
            .eviction().strategy(EvictionStrategy.LRU).build();
   }

   public void testNoEvictionThread() {
      EvictionManagerImpl em = new EvictionManagerImpl();
      Configuration cfg = getCfg().fluent().expiration().wakeUpInterval(0L).build();

      ScheduledExecutorService mockService = createMock(ScheduledExecutorService.class);
      em.initialize(mockService, cfg, null, null, null);
      replay(mockService);
      em.start();

      assert em.evictionTask == null : "Eviction task is not null!  Should not have scheduled anything!";
      verify(mockService); // expect that the executor was never used!!
   }

   public void testWakeupInterval() {
      EvictionManagerImpl em = new EvictionManagerImpl();
      Configuration cfg = getCfg().fluent().expiration().wakeUpInterval(789L).build();

      ScheduledExecutorService mockService = createMock(ScheduledExecutorService.class);
      em.initialize(mockService, cfg, null, null, null);

      ScheduledFuture mockFuture = createNiceMock(ScheduledFuture.class);
      expect(mockService.scheduleWithFixedDelay(isA(EvictionManagerImpl.ScheduledTask.class), eq((long) 789),
                                                eq((long) 789), eq(TimeUnit.MILLISECONDS)))
            .andReturn(mockFuture).once();
      replay(mockService);
      em.start();

      assert em.evictionTask == mockFuture;
      verify(mockService); // expect that the executor was never used!!
   }
}

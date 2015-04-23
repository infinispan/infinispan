package org.infinispan.expiration.impl;

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

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNull;

@Test(groups = "unit", testName = "expiration.impl.ExpirationManagerTest")
public class ExpirationManagerTest extends AbstractInfinispanTest {

   private ConfigurationBuilder getCfg() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      return builder;
   }

   public void testNoEvictionThread() {
      ExpirationManagerImpl em = new ExpirationManagerImpl();
      Configuration cfg = getCfg().expiration().wakeUpInterval(0L).build();

      ScheduledExecutorService mockService = mock(ScheduledExecutorService.class);
      em.initialize(mockService, "", cfg, null, null, null, null);
      em.start();

      assertNull("Expiration task is not null!  Should not have scheduled anything!", em.expirationTask);
   }

   public void testWakeupInterval() {
      ExpirationManagerImpl em = new ExpirationManagerImpl();
      Configuration cfg = getCfg().expiration().wakeUpInterval(789L).build();

      ScheduledExecutorService mockService = mock(ScheduledExecutorService.class);
      em.initialize(mockService, "", cfg, null, null, null, null);

      ScheduledFuture mockFuture = mock(ScheduledFuture.class);
      when(mockService.scheduleWithFixedDelay(isA(ExpirationManagerImpl.ScheduledTask.class), eq(789l),
                                                eq(789l), eq(TimeUnit.MILLISECONDS)))
            .thenReturn(mockFuture);
      em.start();

      assertEquals(mockFuture, em.expirationTask);
      verify(mockService).scheduleWithFixedDelay(any(Runnable.class), anyLong(), anyLong(), any(TimeUnit.class)); // expect that the executor was never used!!
   }
}

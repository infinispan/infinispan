package org.infinispan.expiration.impl;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNull;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.infinispan.AdvancedCache;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.factories.impl.TestComponentAccessors;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.mockito.Answers;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "expiration.impl.ExpirationManagerTest")
public class ExpirationManagerTest extends AbstractInfinispanTest {

   private ConfigurationBuilder getCfg() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      return builder;
   }

   public void testNoExpirationThread() {
      ExpirationManagerImpl em = new ExpirationManagerImpl();
      Configuration cfg = getCfg().expiration().wakeUpInterval(0L).build();

      ScheduledExecutorService mockService = mock(ScheduledExecutorService.class);
      TestingUtil.inject(em, cfg, mock(AdvancedCache.class, Answers.RETURNS_MOCKS),
            new TestComponentAccessors.NamedComponent(KnownComponentNames.EXPIRATION_SCHEDULED_EXECUTOR, mockService));
      em.start();

      assertNull("Expiration task is not null!  Should not have scheduled anything!", em.expirationTask);
   }

   public void testWakeupInterval() {
      ExpirationManagerImpl em = new ExpirationManagerImpl();
      Configuration cfg = getCfg().expiration().wakeUpInterval(789L).build();

      ScheduledExecutorService mockService = mock(ScheduledExecutorService.class);
      TestingUtil.inject(em, cfg, mock(AdvancedCache.class, Answers.RETURNS_MOCKS),
            new TestComponentAccessors.NamedComponent(KnownComponentNames.EXPIRATION_SCHEDULED_EXECUTOR, mockService));

      ScheduledFuture mockFuture = mock(ScheduledFuture.class);
      when(mockService.scheduleWithFixedDelay(isA(ExpirationManagerImpl.ScheduledTask.class), eq(789l),
                                                eq(789l), eq(TimeUnit.MILLISECONDS)))
            .thenReturn(mockFuture);
      em.start();

      assertEquals(mockFuture, em.expirationTask);
      verify(mockService).scheduleWithFixedDelay(any(Runnable.class), anyLong(), anyLong(), any(TimeUnit.class)); // expect that the executor was never used!!
   }
}

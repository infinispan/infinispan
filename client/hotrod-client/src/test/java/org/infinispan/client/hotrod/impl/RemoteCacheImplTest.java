package org.infinispan.client.hotrod.impl;

import static org.junit.Assert.assertEquals;

import java.util.concurrent.TimeUnit;

import org.testng.annotations.Test;

@Test (testName = "client.hotrod.RemoteCacheImplTest", groups = "unit" )
public class RemoteCacheImplTest {
   @Test
   public void testSubsecondConversion() {
      assertEquals(0, RemoteCacheImpl.toSeconds(0, TimeUnit.MILLISECONDS));
      assertEquals(1, RemoteCacheImpl.toSeconds(1, TimeUnit.MILLISECONDS));
      assertEquals(1, RemoteCacheImpl.toSeconds(999, TimeUnit.MILLISECONDS));
      assertEquals(1, RemoteCacheImpl.toSeconds(1000, TimeUnit.MILLISECONDS));
   }

   @Test
   public void testFractionOfSecondConversion() {
      assertEquals(2, RemoteCacheImpl.toSeconds(1001, TimeUnit.MILLISECONDS));
      assertEquals(2, RemoteCacheImpl.toSeconds(1999, TimeUnit.MILLISECONDS));
      assertEquals(2, RemoteCacheImpl.toSeconds(2000, TimeUnit.MILLISECONDS));
      assertEquals(3, RemoteCacheImpl.toSeconds(2001, TimeUnit.MILLISECONDS));
   }
}

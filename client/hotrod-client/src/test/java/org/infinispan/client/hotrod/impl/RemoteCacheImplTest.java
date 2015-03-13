package org.infinispan.client.hotrod.impl;

import static org.junit.Assert.assertEquals;

import java.util.concurrent.TimeUnit;

import org.testng.annotations.Test;

@Test (testName = "client.hotrod.RemoteCacheImplTest", groups = "unit" )
public class RemoteCacheImplTest {
   @Test
   public void testSubsecondConversion() {
      assertEquals(0, RemoteCacheImpl.toSeconds(0, TimeUnit.MILLISECONDS));
      assertEquals(0, RemoteCacheImpl.nanoSecondsRemainder(0, TimeUnit.MILLISECONDS, 0));

      assertEquals(0, RemoteCacheImpl.toSeconds(1, TimeUnit.MILLISECONDS));
      assertEquals(1000000, RemoteCacheImpl.nanoSecondsRemainder(1, TimeUnit.MILLISECONDS, 0));

      assertEquals(0, RemoteCacheImpl.toSeconds(999, TimeUnit.MILLISECONDS));
      assertEquals(999000000, RemoteCacheImpl.nanoSecondsRemainder(999, TimeUnit.MILLISECONDS, 0));

      assertEquals(1, RemoteCacheImpl.toSeconds(1000, TimeUnit.MILLISECONDS));
      assertEquals(0, RemoteCacheImpl.nanoSecondsRemainder(1000, TimeUnit.MILLISECONDS, 1));
   }

   @Test
   public void testFractionOfSecondConversion() {
      assertEquals(1, RemoteCacheImpl.toSeconds(1001, TimeUnit.MILLISECONDS));
      assertEquals(1000000, RemoteCacheImpl.nanoSecondsRemainder(1001, TimeUnit.MILLISECONDS, 1));

      assertEquals(1, RemoteCacheImpl.toSeconds(1999, TimeUnit.MILLISECONDS));
      assertEquals(999000000, RemoteCacheImpl.nanoSecondsRemainder(1999, TimeUnit.MILLISECONDS, 1));

      assertEquals(2, RemoteCacheImpl.toSeconds(2000, TimeUnit.MILLISECONDS));
      assertEquals(0, RemoteCacheImpl.nanoSecondsRemainder(2000, TimeUnit.MILLISECONDS, 2));

      assertEquals(2, RemoteCacheImpl.toSeconds(2001, TimeUnit.MILLISECONDS));
      assertEquals(1000000, RemoteCacheImpl.nanoSecondsRemainder(2001, TimeUnit.MILLISECONDS, 2));
   }
}

package org.infinispan.client.hotrod.impl;

import static org.testng.AssertJUnit.assertEquals;

import java.util.concurrent.TimeUnit;

import org.infinispan.client.hotrod.impl.protocol.CodecUtils;
import org.testng.annotations.Test;

@Test (testName = "client.hotrod.RemoteCacheImplTest", groups = "unit" )
public class RemoteCacheImplTest {
   @Test
   public void testSubsecondConversion() {
      assertEquals(0, CodecUtils.toSeconds(0, TimeUnit.MILLISECONDS));
      assertEquals(1, CodecUtils.toSeconds(1, TimeUnit.MILLISECONDS));
      assertEquals(1, CodecUtils.toSeconds(999, TimeUnit.MILLISECONDS));
      assertEquals(1, CodecUtils.toSeconds(1000, TimeUnit.MILLISECONDS));
   }

   @Test
   public void testFractionOfSecondConversion() {
      assertEquals(2, CodecUtils.toSeconds(1001, TimeUnit.MILLISECONDS));
      assertEquals(2, CodecUtils.toSeconds(1999, TimeUnit.MILLISECONDS));
      assertEquals(2, CodecUtils.toSeconds(2000, TimeUnit.MILLISECONDS));
      assertEquals(3, CodecUtils.toSeconds(2001, TimeUnit.MILLISECONDS));
   }
}

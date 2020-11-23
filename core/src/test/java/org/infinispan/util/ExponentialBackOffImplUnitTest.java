package org.infinispan.util;

import org.infinispan.test.AbstractInfinispanTest;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

/**
 * Unit test for {@link ExponentialBackOffImpl}.
 *
 * @author Pedro Ruivo
 * @since 12.0
 */
@Test(groups = "unit", testName = "util.ExponentialBackOffImplUnitTest")
public class ExponentialBackOffImplUnitTest extends AbstractInfinispanTest {

   public void test() {
      ExponentialBackOffImpl backOff = new ExponentialBackOffImpl(null); // async back off not used
      assertInterval(backOff.nextBackOffMillis(), 250, 750); //500
      assertInterval(backOff.nextBackOffMillis(), 500, 1500); //1000
      assertInterval(backOff.nextBackOffMillis(), 1000, 3000); //2000
      assertInterval(backOff.nextBackOffMillis(), 2000, 6000); //4000
      assertInterval(backOff.nextBackOffMillis(), 4000, 12000); //8000
      assertInterval(backOff.nextBackOffMillis(), 8000, 24000); //16000
      assertInterval(backOff.nextBackOffMillis(), 16000, 48000); //32000
      assertInterval(backOff.nextBackOffMillis(), 32000, 96000); //64000
      assertInterval(backOff.nextBackOffMillis(), 64000, 192000); //128000
      assertInterval(backOff.nextBackOffMillis(), 128000, 300000); //256000
      assertInterval(backOff.nextBackOffMillis(), 300000, 300000); //MAX_INTERVAL_MILLIS
      assertInterval(backOff.nextBackOffMillis(), 300000, 300000); //MAX_INTERVAL_MILLIS
      assertInterval(backOff.nextBackOffMillis(), 300000, 300000); //MAX_INTERVAL_MILLIS

      backOff.reset();

      assertInterval(backOff.nextBackOffMillis(), 250, 750); //500
      assertInterval(backOff.nextBackOffMillis(), 500, 1500); //1000
      assertInterval(backOff.nextBackOffMillis(), 1000, 3000); //2000
      assertInterval(backOff.nextBackOffMillis(), 2000, 6000); //4000
      assertInterval(backOff.nextBackOffMillis(), 4000, 12000); //8000
      assertInterval(backOff.nextBackOffMillis(), 8000, 24000); //16000
      assertInterval(backOff.nextBackOffMillis(), 16000, 48000); //32000
      assertInterval(backOff.nextBackOffMillis(), 32000, 96000); //64000
      assertInterval(backOff.nextBackOffMillis(), 64000, 192000); //128000
      assertInterval(backOff.nextBackOffMillis(), 128000, 300000); //256000
      assertInterval(backOff.nextBackOffMillis(), 300000, 300000); //MAX_INTERVAL_MILLIS
      assertInterval(backOff.nextBackOffMillis(), 300000, 300000); //MAX_INTERVAL_MILLIS
      assertInterval(backOff.nextBackOffMillis(), 300000, 300000); //MAX_INTERVAL_MILLIS
   }

   private void assertInterval(long value, long min, long max) {
      String msg = String.format("%d in [%d, %d]?", value, min, max);
      log.debug(msg);
      AssertJUnit.assertTrue(msg, min <= value && value <= max);
   }

}

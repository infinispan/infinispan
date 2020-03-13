package org.infinispan.commons.util;

public class MyBrokenSampleSPI implements SampleSPI {
   public MyBrokenSampleSPI() {
      throw new RuntimeException("I am broken");
   }
}

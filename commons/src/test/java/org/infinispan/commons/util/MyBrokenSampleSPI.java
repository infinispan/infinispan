package org.infinispan.commons.util;

import org.kohsuke.MetaInfServices;

@MetaInfServices
public class MyBrokenSampleSPI implements SampleSPI {
   public MyBrokenSampleSPI() {
      throw new RuntimeException("I am broken");
   }
}

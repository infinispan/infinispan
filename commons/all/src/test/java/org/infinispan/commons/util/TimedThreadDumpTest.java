package org.infinispan.commons.util;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;
import org.junit.Assert;
import org.junit.Test;

public class TimedThreadDumpTest {

   static {
      Configurator.setLevel("org.infinispan.commons.util.TimedThreadDump", Level.TRACE);
   }

   @Test
   public void dumpThreadsOnlyOnce() {
      Assert.assertTrue(TimedThreadDump.generateThreadDump());
      Assert.assertFalse(TimedThreadDump.generateThreadDump());
   }
}

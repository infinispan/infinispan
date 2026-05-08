package org.infinispan.commons.util;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;
import org.junit.jupiter.api.Test;

public class TimedThreadDumpTest {

   static {
      Configurator.setLevel("org.infinispan.commons.util.TimedThreadDump", Level.TRACE);
   }

   @Test
   public void dumpThreadsOnlyOnce() {
      assertTrue(TimedThreadDump.generateThreadDump());
      assertFalse(TimedThreadDump.generateThreadDump());
   }
}

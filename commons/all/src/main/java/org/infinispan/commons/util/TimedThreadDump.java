package org.infinispan.commons.util;

import java.util.concurrent.TimeUnit;

import org.infinispan.commons.logging.Log;
import org.infinispan.commons.logging.LogFactory;

public class TimedThreadDump {
   private static final Log log = LogFactory.getLog(TimedThreadDump.class);

   private final long interval;
   private long lastUse = 0;

   private TimedThreadDump() {
      this.interval = TimeUnit.SECONDS.toMillis(Long.getLong("infinispan.backpressure.dump.interval.sec", 60));
   }

   private static final TimedThreadDump INSTANCE = new TimedThreadDump();

   public static TimedThreadDump instance() {
      return INSTANCE;
   }

   public static boolean generateThreadDump() {
      return instance().generateThreadDump(System.currentTimeMillis());
   }

   private boolean generateThreadDump(long millisTime) {
      if (log.isTraceEnabled()) {
         boolean dump;
         synchronized (this) {
            dump = lastUse + interval < millisTime;
            if (dump) {
               lastUse = millisTime;
            }
         }

         if (dump) {
            log.trace(Util.threadDump());
            return true;
         }
      }
      return false;
   }
}

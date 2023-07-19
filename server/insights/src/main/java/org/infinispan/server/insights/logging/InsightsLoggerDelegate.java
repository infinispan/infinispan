package org.infinispan.server.insights.logging;

import com.redhat.insights.logging.InsightsLogger;

public class InsightsLoggerDelegate implements InsightsLogger {

   private final Log log;

   public InsightsLoggerDelegate(Log log) {
      this.log = log;
   }

   @Override
   public void debug(String message) {
      log.debug(message);
   }

   @Override
   public void debug(String message, Throwable err) {
      log.debug(message, err);
   }

   @Override
   public void info(String message) {
      log.info(message);
   }

   @Override
   public void error(String message) {
      log.error(message);
   }

   @Override
   public void error(String message, Throwable err) {
      log.error(message, err);
   }

   @Override
   public void warning(String message) {
      log.warn(message);
   }

   @Override
   public void warning(String message, Throwable err) {
      log.warn(message, err);
   }
}

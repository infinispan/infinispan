package org.infinispan.util.logging.log4j;

import java.util.regex.Pattern;

import org.apache.log4j.Level;
import org.apache.log4j.spi.Filter;
import org.apache.log4j.spi.LoggingEvent;

/**
 * Log4j {@link Filter} that only allow events from threads matching a regular expression.
 * Events with a level greater than {@code threshold} are always logged.
 *
 * @author Dan Berindei
 * @since 5.2
 */
public class ThreadNameFilter extends Filter {
   private Level threshold = Level.DEBUG;
   private Pattern includePattern;

   public Level getThreshold() {
      return threshold;
   }

   public void setThreshold(Level threshold) {
      this.threshold = threshold;
   }

   public String getInclude() {
      return includePattern != null ? includePattern.pattern() : null;
   }

   public void setInclude(String include) {
      this.includePattern = Pattern.compile(include);
   }

   @Override
   public int decide(LoggingEvent event) {
      if (event.getLevel().isGreaterOrEqual(threshold)) {
         return Filter.NEUTRAL;
      } else if (includePattern == null || includePattern.matcher(event.getThreadName()).find()) {
         return Filter.NEUTRAL;
      } else {
         return Filter.DENY;
      }
   }
}

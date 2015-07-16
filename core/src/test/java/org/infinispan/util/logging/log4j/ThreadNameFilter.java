package org.infinispan.util.logging.log4j;

import java.util.regex.Pattern;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.Marker;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginFactory;
import org.apache.logging.log4j.core.filter.AbstractFilter;
import org.apache.logging.log4j.message.Message;

/**
 * Log4j {@link Filter} that only allow events from threads matching a regular expression.
 * Events with a level greater than {@code threshold} are always logged.
 *
 * @author Dan Berindei
 * @since 5.2
 */
@Plugin(name = "ThreadNameFilter", category = "Core", elementType = Filter.ELEMENT_TYPE, printObject = true)
public final class ThreadNameFilter extends AbstractFilter {
   /** The serialVersionUID */
   private static final long serialVersionUID = 1L;
   private final Level level;
   private final Pattern includePattern;

   public ThreadNameFilter(Level actualLevel, String includeRegex) {
      this.level = actualLevel;
      this.includePattern = Pattern.compile(includeRegex);
   }

   @Override
   public Result filter(final Logger logger, final Level level, final Marker marker, final String msg,
                        final Object... params) {
       return filter(level, Thread.currentThread().getName());
   }

   @Override
   public Result filter(final Logger logger, final Level level, final Marker marker, final Object msg,
                        final Throwable t) {
       return filter(level, Thread.currentThread().getName());
   }

   @Override
   public Result filter(final Logger logger, final Level level, final Marker marker, final Message msg,
                        final Throwable t) {
       return filter(level, Thread.currentThread().getName());
   }

   @Override
   public Result filter(final LogEvent event) {
      return filter(event.getLevel(), event.getThreadName());
   }

   private Result filter(final Level level, String threadName) {
      if (level.isMoreSpecificThan(this.level)) {
         return Result.NEUTRAL;
      } else if (includePattern == null || includePattern.matcher(threadName).find()) {
         return Result.NEUTRAL;
      } else {
         return Result.DENY;
      }
   }

   @Override
   public String toString() {
       return level.toString();
   }

   /**
    * Create a ThresholdFilter.
    * @param level The log Level.
    * @param match The action to take on a match.
    * @param mismatch The action to take on a mismatch.
    * @return The created ThresholdFilter.
    */
   @PluginFactory
   public static ThreadNameFilter createFilter(
           @PluginAttribute("level") final Level level,
           @PluginAttribute("include") final String include) {
       final Level actualLevel = level == null ? Level.DEBUG : level;
       final String includeRegex = include == null ? ".*" : include;
       return new ThreadNameFilter(actualLevel, includeRegex);
   }
}

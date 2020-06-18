package org.infinispan.commons.logging.log4j;

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

   private final Level level;
   private final Pattern includePattern;

   private ThreadNameFilter(Level level, String includeRegex) {
      this.level = level == null ? Level.DEBUG : level;
      this.includePattern = Pattern.compile(includeRegex == null ? ".*" : includeRegex);
   }

   @Override
   public Result filter(Logger logger, Level level, Marker marker, String msg, Object... params) {
      return filter(level, Thread.currentThread().getName());
   }

   @Override
   public Result filter(Logger logger, Level level, Marker marker, Object msg, Throwable t) {
      return filter(level, Thread.currentThread().getName());
   }

   @Override
   public Result filter(Logger logger, Level level, Marker marker, Message msg, Throwable t) {
      return filter(level, Thread.currentThread().getName());
   }

   @Override
   public Result filter(LogEvent event) {
      return filter(event.getLevel(), event.getThreadName());
   }

   private Result filter(Level level, String threadName) {
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
      return "ThreadNameFilter{level=" + level + ", include=" + includePattern.pattern() + '}';
   }

   /**
    * Create a ThreadNameFilter.
    *
    * @param level   The log Level.
    * @param include The regex
    * @return The created filter.
    */
   @PluginFactory
   public static ThreadNameFilter createFilter(@PluginAttribute("level") Level level,
                                               @PluginAttribute("include") String include) {
      return new ThreadNameFilter(level, include);
   }
}

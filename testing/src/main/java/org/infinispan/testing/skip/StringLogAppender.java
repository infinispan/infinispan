package org.infinispan.testing.skip;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.logging.log4j.core.config.Property;

/**
 * A Log4J Appender which stores logs in a StringBuilder
 *
 * @author Tristan Tarrant
 * @since 9.2
 */
public class StringLogAppender extends AbstractAppender implements Iterable<String> {

   private static final AtomicInteger COUNTER = new AtomicInteger();

   private final String category;
   private final Level level;
   private final List<String> logs;
   private final Predicate<Thread> threadFilter;

   public StringLogAppender(String category, Level level, Predicate<Thread> threadFilter, Layout<?> layout) {
      super(String.format("%s-%s-%d", StringLogAppender.class.getName(), category, COUNTER.incrementAndGet()), null, layout, true, Property.EMPTY_ARRAY);
      this.category = category;
      this.level = level;
      this.logs = Collections.synchronizedList(new ArrayList<>());
      this.threadFilter = threadFilter;
   }

   public void install() {
      LoggerContext loggerContext = (LoggerContext) LogManager.getContext(false);
      Configuration config = loggerContext.getConfiguration();
      this.start();
      config.addAppender(this);

      LoggerConfig loggerConfig = config.getLoggerConfig(category);
      if (!loggerConfig.getName().equals(category)) {
         synchronized (StringLogAppender.class) {
            loggerConfig = config.getLoggerConfig(category);
            if (!loggerConfig.getName().equals(category)) {
               loggerConfig = LoggerConfig.newBuilder()
                     .withAdditivity(true)
                     .withLevel(level)
                     .withLoggerName(category)
                     .withConfig(config)
                     .build();
               config.addLogger(category, loggerConfig);
            }
         }
      }
      loggerConfig.addAppender(this, level, null);
      loggerContext.updateLoggers();
   }

   public void uninstall() {
      LoggerContext loggerContext = (LoggerContext) LogManager.getContext(false);
      Configuration config = loggerContext.getConfiguration();
      LoggerConfig loggerConfig = config.getLoggerConfig(category);
      if (loggerConfig.getName().equals(category)) {
         loggerConfig.removeAppender(this.getName());
      }
      loggerContext.updateLoggers();
   }

   @Override
   public void append(LogEvent event) {
      if (threadFilter.test(Thread.currentThread())) {
         logs.add((String) getLayout().toSerializable(event));
      }
   }

   public int size() {
      return logs.size();
   }

   public String get(int index) {
      if (index < 0) {
         throw new IllegalArgumentException("Index must not be negative.");
      }
      int size = logs.size();
      if (index >= size) {
         throw new IllegalArgumentException("Index " + index + " is out of bounds. "
               + (size == 0 ? "No logs recorded yet." : "Accepted values are: [0 .. " + (size - 1) + "]"));
      }
      return logs.get(index);
   }

   @Override
   public Iterator<String> iterator() {
      return logs.iterator();
   }
}

package org.infinispan.commons.test.skip;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Predicate;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.AppenderRef;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;

/**
 * A Log4J Appender which stores logs in a StringBuilder
 *
 * @author Tristan Tarrant
 * @since 9.2
 */

public class StringLogAppender extends AbstractAppender {

   private final String category;
   private final Level level;
   private final List<String> logs;
   private final Predicate<Thread> threadFilter;

   public StringLogAppender(String category, Level level, Predicate<Thread> threadFilter, Layout layout) {
      super(StringLogAppender.class.getName(), null, layout);
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
      AppenderRef ref = AppenderRef.createAppenderRef(this.getName(), level, null);
      AppenderRef[] refs = new AppenderRef[] {ref};
      LoggerConfig loggerConfig = LoggerConfig.createLogger(true, level, category, null, refs, null, config, null);
      loggerConfig.addAppender(this, null, null);
      config.addLogger(category, loggerConfig);
      loggerContext.updateLoggers();
   }

   public void uninstall() {
      LoggerContext loggerContext = (LoggerContext) LogManager.getContext(false);
      loggerContext.getConfiguration().removeLogger(category);
      loggerContext.updateLoggers();
   }

   @Override
   public void append(LogEvent event) {
      if (threadFilter.test(Thread.currentThread())) {
         logs.add((String) getLayout().toSerializable(event));
      }
   }

   public String getLog(int i) {
      return logs.get(i);
   }
}

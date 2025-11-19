package org.infinispan.cdi.common.util.logging;

import static org.jboss.logging.Logger.Level.INFO;

import java.lang.invoke.MethodHandles;

import org.jboss.logging.BasicLogger;
import org.jboss.logging.Logger;
import org.jboss.logging.annotations.LogMessage;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageLogger;
import org.jboss.logging.annotations.ValidIdRange;

/**
 * The JBoss Logging interface which defined the logging methods for the CDI integration.
 *
 * @author Kevin Pollet &lt;kevin.pollet@serli.com&gt; (C) 2011 SERLI
 */
@MessageLogger(projectCode = "ISPN")
@ValidIdRange(min = 17001, max = 18000)
public interface Log extends BasicLogger {

   static Log getLog(Class<?> clazz) {
      return Logger.getMessageLogger(MethodHandles.lookup(), Log.class, clazz.getName());
   }

   @LogMessage(level = INFO)
   @Message(value = "Infinispan CDI extension version: %s", id = 17001)
   void version(String version);

   @Message(value = "%s parameter must not be null", id = 17003)
   IllegalArgumentException parameterMustNotBeNull(String parameterName);

}

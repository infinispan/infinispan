package org.infinispan.cdi.remote.logging;

import static org.jboss.logging.Logger.Level.INFO;

import org.jboss.logging.BasicLogger;
import org.jboss.logging.annotations.LogMessage;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageLogger;

/**
 * The JBoss Logging interface which defined the logging methods for the CDI integration. The id range for the CDI
 * integration is 17001-18000
 *
 * @author Kevin Pollet &lt;kevin.pollet@serli.com&gt; (C) 2011 SERLI
 */
@MessageLogger(projectCode = "ISPN")
public interface RemoteLog extends BasicLogger {

   @LogMessage(level = INFO)
   @Message(value = "Overriding default remote cache manager not found - adding default implementation", id = 17004)
   void addDefaultRemoteCacheManager();

}

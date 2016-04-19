package org.infinispan.cdi.remote.logging;

import org.jboss.logging.BasicLogger;
import org.jboss.logging.annotations.MessageLogger;

/**
 * The JBoss Logging interface which defined the logging methods for the CDI integration. The id range for the CDI
 * integration is 17001-18000
 *
 * @author Kevin Pollet <kevin.pollet@serli.com> (C) 2011 SERLI
 */
@MessageLogger(projectCode = "ISPN")
public interface RemoteLog extends BasicLogger {

}

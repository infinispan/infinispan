package org.infinispan.rest.embedded.netty4.i18n;

import org.jboss.logging.BasicLogger;
import org.jboss.logging.Logger;
import org.jboss.logging.annotations.MessageLogger;

/**
 * @author <a href="ron.sigal@jboss.com">Ron Sigal</a>
 * @version $Revision: 1.1 $
 *          <p>
 *          Copyright Sep 1, 2015
 *          Temporary fork from RestEasy 3.1.0
 */
@MessageLogger(projectCode = "RESTEASY")
public interface LogMessages extends BasicLogger {
   LogMessages LOGGER = Logger.getMessageLogger(LogMessages.class, LogMessages.class.getPackage().getName());
}

package org.infinispan.commons.jdk22.logging;

import org.jboss.logging.BasicLogger;
import org.jboss.logging.Logger;
import org.jboss.logging.annotations.MessageLogger;
import org.jboss.logging.annotations.ValidIdRange;
import org.jboss.logging.annotations.ValidIdRanges;

/**
 * Keep in sync with the Log class in the commons package. Anything added here must also be placed there and commented.
 * @api.private
 */
@MessageLogger(projectCode = "ISPN")
@ValidIdRanges({
      @ValidIdRange(min = 901, max = 1000),
})
public interface Jdk22Log extends BasicLogger {
   String LOG_ROOT = "org.infinispan.";
   Jdk22Log CONFIG = Logger.getMessageLogger(Jdk22Log.class, LOG_ROOT + "CONFIG");
   Jdk22Log CONTAINER = Logger.getMessageLogger(Jdk22Log.class, LOG_ROOT + "CONTAINER");
   Jdk22Log SECURITY = Logger.getMessageLogger(Jdk22Log.class, LOG_ROOT + "SECURITY");
}

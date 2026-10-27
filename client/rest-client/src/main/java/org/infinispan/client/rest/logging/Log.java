package org.infinispan.client.rest.logging;

import static org.infinispan.commons.logging.Log.LOG_ROOT;

import java.lang.invoke.MethodHandles;

import org.jboss.logging.BasicLogger;
import org.jboss.logging.Logger;
import org.jboss.logging.annotations.MessageLogger;
import org.jboss.logging.annotations.ValidIdRange;
import org.jboss.logging.annotations.ValidIdRanges;

@MessageLogger(projectCode = "ISPN")
@ValidIdRanges({
      @ValidIdRange(min = 9001, max = 10000)
})
public interface Log extends BasicLogger {
   Log REST = Logger.getMessageLogger(MethodHandles.lookup(), Log.class, LOG_ROOT + "REST");
}

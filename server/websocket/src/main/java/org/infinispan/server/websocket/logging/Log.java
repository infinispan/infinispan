package org.infinispan.server.websocket.logging;

import org.jboss.logging.annotations.MessageLogger;

/**
 * Log abstraction for the websocket server. For this module, message ids
 * ranging from 13001 to 14000 inclusively have been reserved.
 *
 * @author Galder Zamarreño
 * @since 5.0
 */
@MessageLogger(projectCode = "ISPN")
public interface Log extends org.infinispan.util.logging.Log {
}

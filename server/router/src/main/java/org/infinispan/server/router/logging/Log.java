package org.infinispan.server.router.logging;

import static org.jboss.logging.Logger.Level.ERROR;
import static org.jboss.logging.Logger.Level.INFO;

import java.lang.invoke.MethodHandles;

import org.infinispan.server.router.RoutingTable;
import org.jboss.logging.BasicLogger;
import org.jboss.logging.Logger;
import org.jboss.logging.annotations.Cause;
import org.jboss.logging.annotations.LogMessage;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageLogger;
import org.jboss.logging.annotations.ValidIdRange;

/**
 * Log abstraction for the Router server module. For this module, message ids ranging from 15001 to 16000 inclusively
 * have been reserved.
 *
 * @author Sebastian Łaskawiec
 */
@MessageLogger(projectCode = "ISPN")
@ValidIdRange(min = 15001, max = 16000)
public interface Log extends BasicLogger {
    Log SERVER = Logger.getMessageLogger(MethodHandles.lookup(), Log.class, org.infinispan.util.logging.Log.LOG_ROOT + "SERVER");

    static Log getLog(Class<?> clazz) {
        return Logger.getMessageLogger(MethodHandles.lookup(), Log.class, clazz.getName());
    }

    @Message(value = "Could not find matching route", id = 15002)
    IllegalArgumentException noRouteFound();

    @LogMessage(level = INFO)
    @Message(value = "Routing table: %s", id = 15005)
    void printOutRoutingTable(RoutingTable routingTable);

    @Message(value = "Configuration validation error", id = 15007)
    IllegalStateException configurationValidationError(@Cause Exception e);

    @Message(value = "Unable to start HotRod router", id = 15008)
    IllegalStateException hotrodRouterStartFailed(@Cause Exception e);

    @Message(value = "Unable to start REST router", id = 15009)
    IllegalStateException restRouterStartFailed(@Cause Exception e);

    @LogMessage(level = ERROR)
    @Message(value = "Error while shutting down the router", id = 15010)
    void errorWhileShuttingDown(@Cause Exception e);
}

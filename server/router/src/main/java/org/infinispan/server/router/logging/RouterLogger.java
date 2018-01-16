package org.infinispan.server.router.logging;

import static org.jboss.logging.Logger.Level.DEBUG;
import static org.jboss.logging.Logger.Level.ERROR;
import static org.jboss.logging.Logger.Level.INFO;

import org.infinispan.server.router.RoutingTable;
import org.jboss.logging.BasicLogger;
import org.jboss.logging.annotations.Cause;
import org.jboss.logging.annotations.LogMessage;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageLogger;

import io.netty.util.DomainNameMapping;

/**
 * Log abstraction for the Hot Rod server module. For this module, message ids ranging from 14000 to 15000 inclusively
 * have been reserved.
 *
 * @author Sebastian Łaskawiec
 */
@MessageLogger(projectCode = "ISPN")
public interface RouterLogger extends BasicLogger {

    @LogMessage(level = DEBUG)
    @Message(value = "Using SNI Handler with domain mapping %s", id = 14001)
    void initializedSni(DomainNameMapping domainNameMapping);

    @Message(value = "Could not find matching route", id = 14002)
    IllegalArgumentException noRouteFound();

    @LogMessage(level = DEBUG)
    @Message(value = "HotRod EndpointRouter listening on %s", id = 14003)
    void hotRodRouterStarted(String address);

    @LogMessage(level = DEBUG)
    @Message(value = "REST EndpointRouter listening on %s", id = 14004)
    void restRouterStarted(String address);

    @LogMessage(level = INFO)
    @Message(value = "Routing table: %s", id = 14005)
    void printOutRoutingTable(RoutingTable routingTable);

    @Message(value = "Configuration validation error", id = 14007)
    IllegalStateException configurationValidationError(@Cause Exception e);

    @Message(value = "Unable to start HotRod router", id = 14008)
    IllegalStateException hotrodRouterStartFailed(@Cause Exception e);

    @Message(value = "Unable to start REST router", id = 14009)
    IllegalStateException restRouterStartFailed(@Cause Exception e);

    @LogMessage(level = ERROR)
    @Message(value = "Error while shutting down the router", id = 14010)
    void errorWhileShuttingDown(@Cause Exception e);
}

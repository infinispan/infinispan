/*
 * JBoss, Home of Professional Open Source
 * Copyright 2011-2013 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA.
 */
package org.infinispan.server.endpoint;

import static org.jboss.logging.Logger.Level.DEBUG;
import static org.jboss.logging.Logger.Level.INFO;
import static org.jboss.logging.Logger.Level.WARN;

import org.jboss.logging.BasicLogger;
import org.jboss.logging.Logger;
import org.jboss.logging.annotations.Cause;
import org.jboss.logging.annotations.LogMessage;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageLogger;
import org.jboss.msc.service.ServiceRegistryException;
import org.jboss.msc.service.StartException;

/**
 * @author Tristan Tarrant
 */
@MessageLogger(projectCode = "JDGS")
public interface EndpointLogger extends BasicLogger {
   String ROOT_LOGGER_CATEGORY = EndpointLogger.class.getPackage().getName();

   /**
    * The root logger.
    */
   EndpointLogger ROOT_LOGGER = Logger.getMessageLogger(EndpointLogger.class, ROOT_LOGGER_CATEGORY);

   /**
    * Logs an informational message indicating that an endpoint is being started.
    *
    * @param protocolName
    *           the name of the protocol.
    */
   @LogMessage(level = INFO)
   @Message(id = 10000, value = "%s starting")
   void endpointStarting(String protocolName);

   /**
    * Logs an informational message indicating that an endpoint has started
    *
    * @param protocolName
    *           the name of the protocol.
    * @param listenAddress
    *           the address on which the protocol is listening
    * @param listenPort
    *           the port on which the protocol is listening
    */
   @LogMessage(level = INFO)
   @Message(id = 10001, value = "%s listening on %s")
   void endpointStarted(String protocolName, String listenAddress);

   /**
    * Logs an informational message indicating that an endpoint has started
    *
    * @param protocolName
    *           the name of the protocol.
    * @param contextPath
    *           the context path for the HTTP protocol
    * @param servletPath
    *           the port on which the protocol is listening
    */
   @LogMessage(level = INFO)
   @Message(id = 10002, value = "%s mapped to %s/%s")
   void httpEndpointStarted(String protocolName, String contextPath, String servletPath);

   @Message(id = 10003, value = "No connector is defined in the endpoint subsystem")
   StartException noConnectorDefined();

   @Message(id = 10004, value = "Failed to start %s")
   StartException failedStart(@Cause Throwable t, String serverName);

   @Message(id = 10005, value = "Failed to instantiate connector %s")
   StartException failedConnectorInstantiation(@Cause Throwable cause, String connectorName);

   @Message(id = 10006, value = "Failed to instantiate transport for connector %s")
   StartException failedTransportInstantiation(@Cause Throwable cause, String connectorName);

   @LogMessage(level = DEBUG)
   @Message(id = 10007, value = "Starting connector %s")
   void connectorStarting(String serverName);

   @LogMessage(level = DEBUG)
   @Message(id = 10008, value = "Stopped connector %s")
   void connectorStopped(String serverName);

   @LogMessage(level = DEBUG)
   @Message(id = 10009, value = "Stopping connector %s")
   void connectorStopping(String serverName);

   @LogMessage(level = WARN)
   @Message(id = 10010, value = "Failed to stop connector %s")
   void connectorStopFailed(@Cause Throwable cause, String serverName);

   @LogMessage(level = INFO)
   @Message(id = 10011, value = "Failed to determine servlet")
   void failedToDetermineServlet(@Cause Throwable cause);

   @LogMessage(level = WARN)
   @Message(id = 10012, value = "Could not stop context")
   void contextStopFailed(@Cause Throwable cause);

   @LogMessage(level = WARN)
   @Message(id = 10013, value = "Could not destroy context")
   void contextDestroyFailed(@Cause Throwable cause);

   @Message(id = 10014, value = "Could not set the CacheManager on the REST Server")
   StartException restCacheManagerInjectionFailed(@Cause Throwable cause);

   @Message(id = 10015, value = "Could not create the web context for the REST Server")
   StartException restContextCreationFailed(@Cause Throwable cause);

   @Message(id = 10016, value = "Could not start the web context for the REST Server")
   StartException restContextStartFailed(@Cause Throwable cause);

   @Message(id = 10017, value = "Failed to locate ServerBootstrap")
   ServiceRegistryException cannotLocateServerBootstrap(@Cause Throwable t);

   @Message(id = 10018, value = "Endpoint '%s' requires SSL, but no SSL context is available in realm '%s'")
   StartException noSSLContext(String endpoint, String realm);

   @LogMessage(level = WARN)
   @Message(id = 10019, value = "The topology update timeout configuration is ignored")
   void topologyUpdateTimeoutIgnored();
}

/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2000 - 2011, Red Hat Middleware LLC, and individual contributors
 * as indicated by the @author tags. See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */

package org.infinispan.client.hotrod.logging;

import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.client.hotrod.impl.transport.tcp.TcpTransport;
import org.infinispan.config.ConfigurationException;
import org.jboss.logging.BasicLogger;
import org.jboss.logging.Cause;
import org.jboss.logging.LogMessage;
import org.jboss.logging.Message;
import org.jboss.logging.MessageLogger;

import java.io.IOException;
import java.net.SocketAddress;
import java.util.Set;

import static org.jboss.logging.Logger.Level.*;

/**
 * Log abstraction for the hot rod client. For this module, message ids
 * ranging from 4001 to 5000 inclusively have been reserved.
 *
 * @author Galder Zamarreño
 * @since 5.0
 */
@MessageLogger(projectCode = "ISPN")
public interface Log extends BasicLogger {

   @LogMessage(level = WARN)
   @Message(value = "Could not find '%s' file in classpath, using defaults.", id = 4001)
   void couldNotFindPropertiesFile(String propertiesFile);

   @LogMessage(level = INFO)
   @Message(value = "Cannot perform operations on a cache associated with an unstarted RemoteCacheManager. Use RemoteCacheManager.start before using the remote cache.", id = 4002)
   void unstartedRemoteCacheManager();

   @LogMessage(level = ERROR)
   @Message(value = "Invalid magic number. Expected %#x and received %#x", id = 4003)
   void invalidMagicNumber(short expectedMagicNumber, short receivedMagic);

   @LogMessage(level = ERROR)
   @Message(value = "Invalid message id. Expected %d and received %d", id = 4004)
   void invalidMessageId(long expectedMsgId, long receivedMsgId);

   @LogMessage(level = WARN)
   @Message(value = "Error received from the server: %s", id = 4005)
   void errorFromServer(String message);

   @LogMessage(level = INFO)
   @Message(value = "%s sent new topology view (id=%d) containing %d addresses: %s", id = 4006)
   void newTopology(SocketAddress address, int viewId, int topologySize, Set<SocketAddress> topology);

   @LogMessage(level = ERROR)
   @Message(value = "Exception encountered. Retry %d out of %d", id = 4007)
   void exceptionAndNoRetriesLeft(int retry, int maxRetries, @Cause HotRodClientException te);

   //  id = 4008 is now logged to TRACE(ISPN-1794)

   @LogMessage(level = WARN)
   @Message(value = "Issues closing socket for %s: %s", id = 4009)
   void errorClosingSocket(TcpTransport transport, IOException e);

   @LogMessage(level = WARN)
   @Message(value = "Exception while shutting down the connection pool.", id = 4010)
   void errorClosingConnectionPool(@Cause Exception e);

   @LogMessage(level = WARN)
   @Message(value = "No hash function configured for version: %d", id = 4011)
   void noHasHFunctionConfigured(int hashFunctionVersion);

   @LogMessage(level = WARN)
   @Message(value = "Could not invalidate connection: %s", id = 4012)
   void couldNoInvalidateConnection(TcpTransport transport, @Cause Exception e);

   @LogMessage(level = WARN)
   @Message(value = "Could not release connection: %s", id = 4013)
   void couldNotReleaseConnection(TcpTransport transport, @Cause Exception e);

   @LogMessage(level = INFO)
   @Message(value = "New server added(%s), adding to the pool.", id = 4014)
   void newServerAdded(SocketAddress server);

   @LogMessage(level = WARN)
   @Message(value = "Failed adding new server %s", id = 4015)
   void failedAddingNewServer(SocketAddress server, @Cause Exception e);

   @LogMessage(level = INFO)
   @Message(value = "Server not in cluster anymore(%s), removing from the pool.", id = 4016)
   void removingServer(SocketAddress server);

   @LogMessage(level = ERROR)
   @Message(value = "Could not fetch transport", id = 4017)
   void couldNotFetchTransport(@Cause Exception e);

   @LogMessage(level = WARN)
   @Message(value = "Unable to convert string property [%s] to an int! Using default value of %d", id = 4018)
   void unableToConvertStringPropertyToInt(String value, int defaultValue);

   @LogMessage(level = WARN)
   @Message(value = "Unable to convert string property [%s] to a long! Using default value of %d", id = 4019)
   void unableToConvertStringPropertyToLong(String value, long defaultValue);

   @LogMessage(level = WARN)
   @Message(value = "Unable to convert string property [%s] to a boolean! Using default value of %b", id = 4020)
   void unableToConvertStringPropertyToBoolean(String value, boolean defaultValue);

   @LogMessage(level = INFO)
   @Message(value = "Infinispan version: %s", id = 4021)
   void version(String version);

   @LogMessage(level = WARN)
   @Message(value = "Unable to invalidate transport for server: %s", id = 4022)
   void unableToInvalidateTransport(SocketAddress serverAddress);

   @Message(value = "SSL Enabled but no KeyStore specified", id = 4023)
   ConfigurationException noSSLKeyManagerConfiguration();

   @Message(value = "SSL Enabled but no TrustStore specified", id = 4024)
   ConfigurationException noSSLTrustManagerConfiguration();

   @Message(value = "A password is required to open the KeyStore '%s'", id = 4025)
   ConfigurationException missingKeyStorePassword(String keyStore);

   @Message(value = "A password is required to open the TrustStore '%s'", id = 4026)
   ConfigurationException missingTrustStorePassword(String trustStore);

   @Message(value = "Cannot configure custom KeyStore and/or TrustStore when specifying a SSLContext", id = 4027)
   ConfigurationException xorSSLContext();
}

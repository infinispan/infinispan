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

package org.infinispan.server.core.logging;

import org.jboss.logging.Cause;
import org.jboss.logging.LogMessage;
import org.jboss.logging.Message;
import org.jboss.logging.MessageLogger;
import org.jboss.netty.channel.Channel;

import java.net.SocketAddress;

import static org.jboss.logging.Logger.Level.*;

/**
 * Log abstraction for the server core module. For this module, message ids
 * ranging from 5001 to 6000 inclusively have been reserved.
 *
 * @author Galder Zamarreño
 * @since 5.0
 */
@MessageLogger(projectCode = "ISPN")
public interface JavaLog extends org.infinispan.util.logging.Log {

   @LogMessage(level = INFO)
   @Message(value = "Start main with args: %s", id = 5001)
   void startWithArgs(String args);

   @LogMessage(level = INFO)
   @Message(value = "Posting Shutdown Request to the server...", id = 5002)
   void postingShutdownRequest();

   @LogMessage(level = ERROR)
   @Message(value = "Exception reported", id = 5003)
   void exceptionReported(@Cause Throwable t);

   @LogMessage(level = WARN)
   @Message(value = "Server channel group did not completely unbind", id = 5004)
   void serverDidNotUnbind();

   @LogMessage(level = WARN)
   @Message(value = "%s is still bound to %s", id = 5005)
   void channelStillBound(Channel ch, SocketAddress address);

   @LogMessage(level = WARN)
   @Message(value = "Channel group did not completely close", id = 5006)
   void serverDidNotClose();

   @LogMessage(level = WARN)
   @Message(value = "%s is still connected to %s", id = 5007)
   void channelStillConnected(Channel ch, SocketAddress address);

   @LogMessage(level = WARN)
   @Message(value = "Setting the number of master threads is no longer supported", id = 5008)
   void settingMasterThreadsNotSupported();

   @LogMessage(level = ERROR)
   @Message(value = "Unexpected error before any request parameters read", id = 5009)
   void errorBeforeReadingRequest(@Cause Throwable t);

}

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

package org.infinispan.server.core.logging

import org.jboss.netty.channel.Channel
import java.net.SocketAddress
import org.infinispan.util.logging.LogFactory

/**
 * A logging facade for Scala code.
 *
 * @author Galder Zamarreño
 * @since 5.0
 */
trait Log {
   private lazy val log: JavaLog = LogFactory.getLog(getClass, classOf[JavaLog])

   def debug(msg: => String) = log.debug(msg)

   def debug(msg: => String, param1: Any) = log.debugf(msg, param1)

   def debug(t: Throwable, msg: => String, param1: Any) = log.debugf(t, msg, param1)

   def debug(msg: => String, param1: Any, param2: Any) =
      log.debugf(msg, param1, param2)

   def debugf(msg: => String, params: Any*) =
      log.debugf(msg, params.map(_.asInstanceOf[AnyRef]) : _*)

   def trace(msg: => String) = log.tracef(msg)

   def trace(msg: => String, param1: Any) = log.tracef(msg, param1)

   def trace(msg: => String, param1: Any, param2: Any) =
      log.tracef(msg, param1, param2)

   def trace(msg: => String, param1: Any, param2: Any, param3: Any) =
      log.tracef(msg, param1, param2, param3)

   def isDebugEnabled = log.isDebugEnabled

   def isTraceEnabled = log.isTraceEnabled

   // INFO or higher level messages support internationalization

   def logStartWithArgs(args: String) = log.startWithArgs(args)

   def logPostingShutdownRequest = log.postingShutdownRequest

   def logExceptionReported(t: Throwable) = log.exceptionReported(t)

   def logServerDidNotUnbind = log.serverDidNotUnbind

   def logChannelStillBound(ch: Channel, address: SocketAddress) =
      log.channelStillBound(ch, address)

   def logServerDidNotClose = log.serverDidNotClose

   def logChannelStillConnected(ch: Channel, address: SocketAddress) =
      log.channelStillConnected(ch, address)

   def logSettingMasterThreadsNotSupported = log.settingMasterThreadsNotSupported

   def logErrorBeforeReadingRequest(t: Throwable) =
      log.errorBeforeReadingRequest(t)
}

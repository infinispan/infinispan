package org.infinispan.server.core.logging

import java.net.SocketAddress
import org.infinispan.util.logging.LogFactory
import io.netty.channel.Channel

/**
 * A logging facade for Scala code.
 *
 * @author Galder Zamarreño
 * @since 5.0
 */
trait Log {
   private lazy val log: JavaLog = LogFactory.getLog(getClass, classOf[JavaLog])

   def info(msg: => String) = log.info(msg)

   def info(msg: => String, param1: Any) = log.infof(msg, param1)

   def error(msg: => String, t: Throwable) = log.errorf(t, msg)

   def warn(msg: => String, t: Throwable) = log.warnf(t, msg)

   def debug(msg: => String) = log.debug(msg)

   def debug(msg: => String, param1: Any) = log.debugf(msg, param1)

   def debug(t: Throwable, msg: => String) = log.debugf(t, msg)

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

   def tracef(msg: => String, params: Any*) =
      log.tracef(msg, params.map(_.asInstanceOf[AnyRef]) : _*)

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

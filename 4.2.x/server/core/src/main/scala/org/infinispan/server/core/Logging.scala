package org.infinispan.server.core

import org.infinispan.util.logging.{LogFactory, Log}

/**
 * A logging facade for scala code.
 *
 * @author Galder Zamarreño
 * @since 4.1
 */
trait Logging {
   private lazy val log: Log = LogFactory.getLog(getClass)

   // params.map(_.asInstanceOf[AnyRef]) => returns a Seq[AnyRef]
   // the ': _*' part tells the compiler to pass it as varargs
   def info(msg: => String, params: Any*) = log.info(msg, params.map(_.asInstanceOf[AnyRef]) : _*)

   def isDebugEnabled = log.isDebugEnabled

   def debug(msg: => String, params: Any*) = log.debug(msg, params.map(_.asInstanceOf[AnyRef]) : _*)

   def isTraceEnabled = log.isTraceEnabled

   def trace(msg: => String, params: Any*) = log.trace(msg, params.map(_.asInstanceOf[AnyRef]) : _*)

   def warn(msg: => String, params: Any*) = log.warn(msg, params.map(_.asInstanceOf[AnyRef]) : _*)

   def warn(msg: => String, t: Throwable) = log.warn(msg, t, null)

   def error(msg: => String) = log.error(msg, null)

   def error(msg: => String, t: Throwable) = log.error(msg, t, null)

}
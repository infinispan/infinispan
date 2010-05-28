package org.infinispan.server.core

import org.infinispan.util.logging.{LogFactory, Log}

/**
 * // TODO: Document this
 * @author Galder Zamarreño
 * @since 4.1
 */
trait Logging {
   private lazy val log: Log = LogFactory.getLog(getClass)

//   def info(msg: => String) = if (log.isInfoEnabled) log.info(msg, null)

   // params.map(_.asInstanceOf[AnyRef]) => returns a Seq[AnyRef]
   // the ': _*' part tells the compiler to pass it as varargs
   def info(msg: => String, params: Any*) = log.info(msg, params.map(_.asInstanceOf[AnyRef]) : _*)

//   def debug(msg: => String) = log.debug(msg, null)

   def isDebugEnabled = log.isDebugEnabled

   def debug(msg: => String, params: Any*) = log.debug(msg, params.map(_.asInstanceOf[AnyRef]) : _*)

//   def trace(msg: => String) = log.trace(msg, null)

   def isTraceEnabled = log.isTraceEnabled

   def trace(msg: => String, params: Any*) = log.trace(msg, params.map(_.asInstanceOf[AnyRef]) : _*)

   def warn(msg: => String, params: Any*) = log.warn(msg, params.map(_.asInstanceOf[AnyRef]) : _*)

   def warn(msg: => String, t: Throwable) = log.warn(msg, t, null)

   def error(msg: => String) = log.error(msg, null)

   def error(msg: => String, t: Throwable) = log.error(msg, t, null)

   // TODO: Sort out the other error methods that take both Throwable and varargs

//   def error(msg: => String, params: Any*) =
//      if (log.isErrorEnabled) log.error(msg, params.map(_.asInstanceOf[AnyRef]) : _*)
//
//   def error(msg: => String, t: Throwable, params: Any*) =
//      if (log.isErrorEnabled) log.error(msg, t, params.map(_.asInstanceOf[AnyRef]) : _*)

}
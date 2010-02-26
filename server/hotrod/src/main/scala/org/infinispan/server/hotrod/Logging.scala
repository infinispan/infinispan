package org.infinispan.server.hotrod

import org.infinispan.util.logging.{Log, LogFactory}
import collection.mutable.WrappedArray

/**
 * TODO: This would be simplest way to get logging in but unfortunately it creates
 * a new Log instance per object created 
 *
 * // TODO: Document this
 * @author Galder Zamarreño
 * @since 4.1
 */
trait Logging {
   private lazy val log: Log = LogFactory.getLog(getClass)

   def info(msg: => String) = log.info(msg, null)

   def info(msg: => String, params: Any*) =
      // params.map(_.asInstanceOf[AnyRef]) => returns a Seq[AnyRef]
      // the ': _*' part tells the compiler to pass it as varargs
      if (log.isInfoEnabled) log.info(msg, params.map(_.asInstanceOf[AnyRef]) : _*)

   def trace(msg: => String) = log.trace(msg, null)

   def trace(msg: => String, params: Any*) =
      if (log.isTraceEnabled) log.trace(msg, params.map(_.asInstanceOf[AnyRef]) : _*)

   def error(msg: => String) = log.error(msg, null)

   def error(msg: => String, t: Throwable) = log.error(msg, t, null)

   // TODO: Sort out the other error methods that take both Throwable and varargs

//   def error(msg: => String, params: Any*) =
//      if (log.isErrorEnabled) log.error(msg, params.map(_.asInstanceOf[AnyRef]) : _*)
//
//   def error(msg: => String, t: Throwable, params: Any*) =
//      if (log.isErrorEnabled) log.error(msg, t, params.map(_.asInstanceOf[AnyRef]) : _*)

}
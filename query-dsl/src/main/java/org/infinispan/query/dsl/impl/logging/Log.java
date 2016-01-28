package org.infinispan.query.dsl.impl.logging;

import org.jboss.logging.BasicLogger;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageLogger;

/**
 * Log abstraction for the query DSL. For this module, message ids
 * ranging from 14801 to 15000 inclusively have been reserved.
 *
 * @author anistor@redhat.com
 * @since 8.2
 */
@MessageLogger(projectCode = "ISPN")
public interface Log extends BasicLogger {

   @Message(value = "Argument cannot be null", id = 14801)
   IllegalArgumentException argumentCannotBeNull();

   @Message(value = "'%s' must be an instance of java.lang.Comparable", id = 14802)
   IllegalArgumentException argumentMustBeComparable(String argName);

   @Message(value = "Parameter name cannot be null or empty", id = 14803)
   IllegalArgumentException parameterNameCannotBeNulOrEmpty();
}

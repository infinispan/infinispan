package org.infinispan.hibernate.search.logging;

import org.hibernate.search.exception.SearchException;
import org.jboss.logging.annotations.Cause;
import org.jboss.logging.annotations.LogMessage;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageLogger;

import javax.naming.NamingException;

import static org.jboss.logging.Logger.Level.ERROR;
import static org.jboss.logging.Logger.Level.WARN;

/**
 * Hibernate Search Infinispan's log abstraction layer on top of JBoss Logging.
 *
 * @author Davide D'Alto
 * @since 4.0
 */
@MessageLogger(projectCode = "ISPN")
public interface Log extends org.infinispan.commons.logging.Log {

   @LogMessage(level = ERROR)
   @Message(id = 26001, value = "Unable to retrieve CacheManager from JNDI [%s]")
   void unableToRetrieveCacheManagerFromJndi(String jndiNamespace, @Cause NamingException ne);

   @LogMessage(level = ERROR)
   @Message(id = 26002, value = "Unable to release initial context")
   void unableToReleaseInitialContext(@Cause NamingException ne);

   @LogMessage(level = WARN)
   @Message(id = 26003, value = "Interrupted while waiting for asynchronous delete operations to be flushed on the index. "
         + "Some stale segments might remain in the index.")
   void interruptedWhileWaitingForAsyncDeleteFlush();

   @LogMessage(level = ERROR)
   @Message(id = 26004, value = "Unable to properly close Lucene directory %1$s")
   void unableToCloseLuceneDirectory(Object directory, @Cause Exception e);

   @Message(id = 26005, value = "Configuration property '%s' should not be empty: illegal format.")
   SearchException configurationPropertyCantBeEmpty(String key);

   @Message(id = 26006, value = "%s")
   SearchException getInvalidIntegerValueException(String msg, @Cause Throwable throwable);
}

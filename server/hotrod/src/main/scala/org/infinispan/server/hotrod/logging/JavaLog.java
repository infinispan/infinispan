package org.infinispan.server.hotrod.logging;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.equivalence.Equivalence;
import org.infinispan.util.concurrent.IsolationLevel;
import org.jboss.logging.Cause;
import org.jboss.logging.LogMessage;
import org.jboss.logging.Message;
import org.jboss.logging.MessageLogger;

import static org.jboss.logging.Logger.Level.*;

/**
 * Log abstraction for the Hot Rod server module. For this module, message ids
 * ranging from 6001 to 7000 inclusively have been reserved.
 *
 * @author Galder Zamarreño
 * @since 5.0
 */
@MessageLogger(projectCode = "ISPN")
public interface JavaLog extends org.infinispan.util.logging.Log {

   @LogMessage(level = WARN)
   @Message(value = "While trying to detect a crashed member, current view returned null", id = 6000)
   void viewNullWhileDetectingCrashedMember();

   @LogMessage(level = WARN)
   @Message(value = "Unable to update topology view after a crashed member left, wait for next view change.", id = 6001)
   void unableToUpdateView();

   @LogMessage(level = ERROR)
   @Message(value = "Error detecting crashed member", id = 6002)
   void errorDetectingCrashedMember(@Cause Throwable t);

   @Message(value = "A topology cache named '%s' has already been defined", id = 6003)
   CacheConfigurationException invalidTopologyCache(String topologyCacheName);

   @Message(value = "Key equivalence must be able to compare arrays based on contents. Provided key equivalence instance '%s' is not possible to do so", id = 6004)
   CacheConfigurationException invalidKeyEquivalence(Equivalence keyEq);

   @Message(value = "Value equivalence must be able to compare arrays based on contents. Provided value equivalence instance '%s' is not possible to do so", id = 6005)
   CacheConfigurationException invalidValueEquivalence(Equivalence keyEq);

   @Message(value = "Isolation level must be READ_COMMITTED or lower: '%s'", id = 6006)
   CacheConfigurationException invalidIsolationLevel(IsolationLevel isolationLevel);

}

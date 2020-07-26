package org.infinispan.anchored.impl;

import org.infinispan.commons.CacheConfigurationException;
import org.jboss.logging.BasicLogger;
import org.jboss.logging.Logger;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageLogger;

/**
 * This module reserves range 30001 - 30500
 */
@MessageLogger(projectCode = "ISPN")
public interface Log extends BasicLogger {
   Log CONFIG = Logger.getMessageLogger(Log.class, org.infinispan.util.logging.Log.LOG_ROOT + "CONFIG");

   @Message(value = "Anchored keys requires cache to be in replication mode", id = 30001)
   CacheConfigurationException replicationModeRequired();

   @Message(value = "Anchored keys do not support transactions, please remove the <transaction> element from your configuration", id = 30002)
   CacheConfigurationException transactionsNotSupported();

   @Message(value = "Anchored keys caches must have state transfer enabled", id = 30003)
   CacheConfigurationException stateTransferRequired();

   @Message(value = "Anchored keys do not support awaiting for state transfer when starting a cache, please remove " +
                    "the await-initial-transfer attribute from your configuration", id = 30004)
   CacheConfigurationException awaitInitialTransferNotSupported();

   @Message(value = "Anchored keys only support partition handling mode ALLOW_READ_WRITES, please remove the " +
                    "when-split attribute from your configuration", id = 30005)
   CacheConfigurationException whenSplitNotSupported();

   @Message(value = "Anchored keys only support merge policy PREFERRED_NON_NULL, please remove the merge-policy " +
                    "attribute from your configuration", id = 30006)
   CacheConfigurationException mergePolicyNotSupported();
}

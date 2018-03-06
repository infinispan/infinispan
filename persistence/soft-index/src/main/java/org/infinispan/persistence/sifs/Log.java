package org.infinispan.persistence.sifs;

import java.io.IOException;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.persistence.spi.PersistenceException;
import org.jboss.logging.BasicLogger;
import org.jboss.logging.Logger;
import org.jboss.logging.annotations.Cause;
import org.jboss.logging.annotations.LogMessage;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageLogger;

/**
 * This module reserves range 29001 - 29500
 */
@MessageLogger(projectCode = "ISPN")
public interface Log extends BasicLogger {

   @Message(value = "Max size of index node (%d) is limited to 32767 bytes.", id = 29001)
   CacheConfigurationException maxNodeSizeLimitedToShort(int maxNodeSize);

   @Message(value = "Min size of index node (%d) must be less or equal to max size (%d).", id = 29002)
   CacheConfigurationException minNodeSizeMustBeLessOrEqualToMax(int minNodeSize, int maxNodeSize);

   @Message(value = "Calculation of size has been interrupted.", id = 29003)
   PersistenceException sizeCalculationInterrupted(@Cause InterruptedException e);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(value = "Failed processing task for key %s", id = 29004)
   void failedProcessingTask(Object key, @Cause Exception e);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(value = "Iteration was interrupted.", id = 29005)
   void iterationInterrupted(@Cause InterruptedException e);

   @LogMessage(level = Logger.Level.WARN)
   @Message(value = "Cannot truncate index", id = 29006)
   void cannotTruncateIndex(@Cause IOException e);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(value = "Unexpected error in index updater thread.", id = 29007)
   void errorInIndexUpdater(@Cause Throwable e);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(value = "Failed to close the index file.", id = 29008)
   void failedToCloseIndex(@Cause IOException e);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(value = "Unexpected error in data compactor.", id = 29009)
   void compactorFailed(@Cause Throwable e);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(value = "Cannot close/delete data file %d.", id = 290010)
   void cannotCloseDeleteFile(int fileId, @Cause IOException e);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(value = "Cannot close data file.", id = 29011)
   void cannotCloseFile(@Cause IOException e);

   @Message(value = "Compaction threshold (%f) should be between 0 (exclusively) and 1 (inclusively).", id = 29012)
   CacheConfigurationException invalidCompactionThreshold(double value);

   @Message(value = "Cannot open index on %s", id = 29013)
   PersistenceException cannotOpenIndex(String location, @Cause IOException e);

   @Message(value = "Interrupted while stopping the store", id = 29014)
   PersistenceException interruptedWhileStopping(@Cause InterruptedException e);

   @Message(value = "Interrupted while pausing the index for clear.", id = 29015)
   PersistenceException interruptedWhileClearing(@Cause InterruptedException e);

   @Message(value = "Cannot clear/reopen index.", id = 29016)
   PersistenceException cannotClearIndex(@Cause IOException e);

   @Message(value = "Cannot clear data directory.", id = 29017)
   PersistenceException cannotClearData(@Cause IOException e);

   @Message(value = "The serialized form of key %s is too long (%d); with maxNodeSize=%d bytes you can use only keys serialized to at most %d bytes.", id = 29018)
   PersistenceException keyIsTooLong(Object key, int keyLength, int maxNodeSize, int maxKeyLength);

   @Message(value = "Cannot load key %s from index.", id = 29019)
   PersistenceException cannotLoadKeyFromIndex(Object key, @Cause Exception e);

   @Message(value = "Index looks corrupt.", id = 29020)
   PersistenceException indexLooksCorrupt(@Cause Exception e);
}

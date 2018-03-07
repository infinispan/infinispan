package org.infinispan.counter.logging;

import java.io.File;

import org.infinispan.configuration.parsing.ParserScope;
import org.infinispan.counter.exception.CounterConfigurationException;
import org.infinispan.counter.exception.CounterException;
import org.infinispan.util.ByteString;
import org.jboss.logging.BasicLogger;
import org.jboss.logging.annotations.Cause;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageLogger;

/**
 * range: 28001 - 29000
 *
 * @author Pedro Ruivo
 * @since 9.0
 */
@MessageLogger(projectCode = "ISPN")
public interface Log extends BasicLogger {

   //28001 is in commons log

//   @Message(value = "The counter was deleted.", id = 28002)
//   CounterException counterDeleted();

   @Message(value = "The counter name is missing.", id = 28003)
   CounterConfigurationException missingCounterName();

   @Message(value = "Invalid storage mode. It must be non-null", id = 28004)
   CounterConfigurationException invalidStorageMode();

   @Message(value = "Invalid storage mode. PERSISTENT is not allowed without global state enabled in cache container.", id = 28005)
   CounterConfigurationException invalidPersistentStorageMode();

   @Message(value = "Invalid number of owner. It must be higher than zero but it was %s", id = 28006)
   CounterConfigurationException invalidNumOwners(int value);

   @Message(value = "Invalid reliability mode. It must be non-null", id = 28007)
   CounterConfigurationException invalidReliabilityMode();

   @Message(value = "Invalid initial value for counter. It must be between %s and %s (inclusive) but was %s", id = 28008)
   CounterConfigurationException invalidInitialValueForBoundedCounter(long lower, long upper, long value);

   @Message(value = "Invalid concurrency-level. It must be higher than zero but it was %s", id = 28009)
   CounterConfigurationException invalidConcurrencyLevel(int value);

//   @LogMessage(level = WARN)
//   @Message(value = "Unable to add(%s) to non-existing counter '%s'.", id = 28010)
//   void noSuchCounterAdd(long value, ByteString counterName);

//   @LogMessage(level = WARN)
//   @Message(value = "Unable to reset non-existing counter '%s'.", id = 28011)
//   void noSuchCounterReset(ByteString counterName);

//   @LogMessage(level = WARN)
//   @Message(value = "Interrupted while waiting for the counter manager caches.", id = 28012)
//   void interruptedWhileWaitingForCaches();

//   @LogMessage(level = ERROR)
//   @Message(value = "Exception while waiting for counter manager caches.", id = 28013)
//   void exceptionWhileWaitingForCached(@Cause Throwable cause);

   @Message(value = "Invalid counter type. Expected=%s but got %s", id = 28014)
   CounterException invalidCounterType(String expected, String actual);

   @Message(value = "Unable to fetch counter manager caches.", id = 28015)
   CounterException unableToFetchCaches();

   @Message(value = "Counter '%s' is not defined.", id = 28016)
   CounterException undefinedCounter(String name);

   @Message(value = "Duplicated counter name found. Counter '%s' already exists.", id = 28017)
   CounterConfigurationException duplicatedCounterName(String counter);

   @Message(value = "Metadata not found but counter exists. Counter=%s", id = 28018)
   IllegalStateException metadataIsMissing(ByteString counterName);

//   @LogMessage(level = WARN)
//   @Message(value = "Unable to compare-and-set(%d, %d) non-existing counter '%s'.", id = 28019)
//   void noSuchCounterCAS(long expected, long value, ByteString counterName);

   @Message(value = "Invalid scope for tag <counter>. Expected CACHE_CONTAINER but was %s", id = 28020)
   CounterConfigurationException invalidScope(ParserScope scope);

//   @Message(value = "Clustered counters only available with clustered cache manager.", id = 28021)
//   CounterException expectedClusteredEnvironment();

   //28022 is in commons log

   //28023 is in hot rod log

   @Message(value = "Lower bound (%s) and upper bound (%s) can't be the same.", id = 28024)
   CounterConfigurationException invalidSameLowerAndUpperBound(long lower, long upper);

   @Message(value = "Cannot rename file %s to %s", id = 28025)
   CounterConfigurationException cannotRenamePersistentFile(String absolutePath, File persistentFile);

   @Message(value = "Error while persisting counter's configurations", id = 28026)
   CounterConfigurationException errorPersistingCountersConfiguration(@Cause Throwable cause);

   @Message(value = "Error while reading counter's configurations", id = 28027)
   CounterConfigurationException errorReadingCountersConfiguration(@Cause Throwable cause);

   @Message(value = "CounterManager hasn't started yet or has been stopped.", id = 28028)
   CounterException managerNotStarted();
}

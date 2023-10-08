package org.infinispan.util.logging;

import static org.jboss.logging.Logger.Level.DEBUG;
import static org.jboss.logging.Logger.Level.ERROR;
import static org.jboss.logging.Logger.Level.FATAL;
import static org.jboss.logging.Logger.Level.INFO;
import static org.jboss.logging.Logger.Level.TRACE;
import static org.jboss.logging.Logger.Level.WARN;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.file.Path;
import java.security.Permission;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.CacheListenerException;
import org.infinispan.commons.CrossSiteIllegalLifecycleStateException;
import org.infinispan.commons.IllegalLifecycleStateException;
import org.infinispan.commons.TimeoutException;
import org.infinispan.commons.configuration.io.Location;
import org.infinispan.commons.dataconversion.EncodingException;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.marshall.MarshallingException;
import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.TypedProperties;
import org.infinispan.commons.util.concurrent.FileSystemLock;
import org.infinispan.configuration.cache.BackupFailurePolicy;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.StorageType;
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.interceptors.impl.ContainerFullException;
import org.infinispan.jmx.JmxDomainConflictException;
import org.infinispan.logging.annotations.Description;
import org.infinispan.partitionhandling.AvailabilityException;
import org.infinispan.partitionhandling.AvailabilityMode;
import org.infinispan.persistence.spi.NonBlockingStore;
import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.remoting.RemoteException;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.jgroups.SuspectException;
import org.infinispan.topology.CacheJoinException;
import org.infinispan.topology.CacheTopology;
import org.infinispan.topology.MissingMembersException;
import org.infinispan.transaction.WriteSkewException;
import org.infinispan.transaction.impl.LocalTransaction;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.transaction.xa.InvalidTransactionException;
import org.infinispan.transaction.xa.recovery.RecoveryAwareRemoteTransaction;
import org.infinispan.transaction.xa.recovery.RecoveryAwareTransaction;
import org.infinispan.util.ByteString;
import org.jboss.logging.BasicLogger;
import org.jboss.logging.Logger;
import org.jboss.logging.annotations.Cause;
import org.jboss.logging.annotations.LogMessage;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageLogger;
import org.jboss.logging.annotations.Once;
import org.jboss.logging.annotations.Param;
import org.jboss.logging.annotations.ValidIdRange;
import org.jgroups.View;

import jakarta.transaction.TransactionManager;

/**
 * Infinispan's log abstraction layer on top of JBoss Logging.
  * It contains explicit methods for all INFO or above levels so that they can
 * be internationalized. For the core module, message ids ranging from 0001
 * to 0900 inclusively have been reserved.
  * <code> Log log = LogFactory.getLog( getClass() ); </code> The above will get
 * you an instance of <code>Log</code>, which can be used to generate log messages
 * either via JBoss Logging which then can delegate to Log4J (if the libraries
 * are present) or (if not) the built-in JDK logger.
  * In addition to the 6 log levels available, this framework also supports
 * parameter interpolation, similar to the JDKs {@link String#format(String, Object...)}
 * method. What this means is, that the following block:
 * <code> if (log.isTraceEnabled()) { log.trace("This is a message " + message + " and some other value is " + value); }
 * </code>
  * ... could be replaced with ...
  * <code> if (log.isTraceEnabled()) log.tracef("This is a message %s and some other value is %s", message, value);
 * </code>
  * This greatly enhances code readability.
  * If you are passing a <code>Throwable</code>, note that this should be passed in
 * <i>before</i> the vararg parameter list.
  *
 * @author Manik Surtani
 * @api.private
 * @since 4.0
 */
@MessageLogger(projectCode = "ISPN")
@ValidIdRange(min = 1, max = 900)
public interface Log extends BasicLogger {
   String LOG_ROOT = "org.infinispan.";
   Log CONFIG = Logger.getMessageLogger(Log.class, LOG_ROOT + "CONFIG");
   Log CLUSTER = Logger.getMessageLogger(Log.class, LOG_ROOT + "CLUSTER");
   Log CONTAINER = Logger.getMessageLogger(Log.class, LOG_ROOT + "CONTAINER");
   Log PERSISTENCE = Logger.getMessageLogger(Log.class, LOG_ROOT + "PERSISTENCE");
   Log SECURITY = Logger.getMessageLogger(Log.class, LOG_ROOT + "SECURITY");
   Log XSITE = Logger.getMessageLogger(Log.class, LOG_ROOT + "XSITE");

//   @LogMessage(level = WARN)
//   @Message(value = "Unable to load %s from cache loader", id = 1)
//   void unableToLoadFromCacheLoader(Object key, @Cause PersistenceException cle);

//   @LogMessage(level = WARN)
//   @Message(value = "Field %s not found!!", id = 2)
//   void fieldNotFound(String fieldName);

//   @LogMessage(level = WARN)
//   @Message(value = "Property %s could not be replaced as intended!", id = 3)
//   void propertyCouldNotBeReplaced(String line);

//   @LogMessage(level = WARN)
//   @Message(value = "Unexpected error reading properties", id = 4)
//   void errorReadingProperties(@Cause IOException e);
//
//   @LogMessage(level = WARN)
//   @Message(value = "Detected write skew on key [%s]. Another process has changed the entry since we last read it! Unable to copy entry for update.", id = 5)
//   void unableToCopyEntryForUpdate(Object key);

//   @LogMessage(level = WARN)
//   @Message(value = "Failed remote execution on node %s", id = 6)
//   void remoteExecutionFailed(Address address, @Cause Throwable t);

//   @LogMessage(level = WARN)
//   @Message(value = "Failed local execution ", id = 7)
//   void localExecutionFailed(@Cause Throwable t);

//   @LogMessage(level = WARN)
//   @Message(value = "Can not select %s random members for %s", id = 8)
//   void cannotSelectRandomMembers(int numNeeded, List<Address> members);

//   @LogMessage(level = INFO)
//   @Message(value = "DistributionManager not yet joined the cluster. Cannot do anything about other concurrent joiners.", id = 14)
//   void distributionManagerNotJoined();

//   @LogMessage(level = WARN)
//   @Message(value = "DistributionManager not started after waiting up to 5 minutes! Not rehashing!", id = 15)
//   void distributionManagerNotStarted();

   @LogMessage(level = ERROR)
   @Message(value = "Problem encountered when applying state for key %s!", id = 16)
   void problemApplyingStateForKey(Object key, @Cause Throwable t);

//   @LogMessage(level = WARN)
//   @Message(value = "Unable to apply prepare %s", id = 18)
//   void unableToApplyPrepare(PrepareCommand pc, @Cause Throwable t);

//   @LogMessage(level = INFO)
//   @Message(value = "Couldn't acquire shared lock", id = 19)
//   void couldNotAcquireSharedLock();

   @LogMessage(level = WARN)
   @Message(value = "Expected just one response; got %s", id = 21)
   void expectedJustOneResponse(Map<Address, Response> lr);

   @LogMessage(level = INFO)
   @Message(value = "wakeUpInterval is <= 0, not starting expired purge thread", id = 25)
   void notStartingEvictionThread();

   @LogMessage(level = WARN)
   @Message(value = "Caught exception purging data container!", id = 26)
   void exceptionPurgingDataContainer(@Cause Throwable e);

//   @LogMessage(level = WARN)
//   @Message(value = "Could not acquire lock for eviction of %s", id = 27)
//   void couldNotAcquireLockForEviction(Object key, @Cause Exception e);

   @LogMessage(level = WARN)
   @Message(value = "Unable to passivate entry under %s", id = 28)
   void unableToPassivateEntry(Object key, @Cause Throwable e);

//   @LogMessage(level = INFO)
//   @Message(value = "Passivating all entries to disk", id = 29)
//   void passivatingAllEntries();
//
//   @LogMessage(level = INFO)
//   @Message(value = "Passivated %d entries in %s", id = 30)
//   void passivatedEntries(long numEntries, String duration);

//   @LogMessage(level = TRACE)
//   @Message(value = "MBeans were successfully registered to the platform MBean server.", id = 31)
//   void mbeansSuccessfullyRegistered();

//   @LogMessage(level = WARN)
//   @Message(value = "Problems un-registering MBeans", id = 32)
//   void problemsUnregisteringMBeans(@Cause Exception e);

//   @LogMessage(level = WARN)
//   @Message(value = "Unable to unregister MBean %s", id = 33)
//   void unableToUnregisterMBean(String name, @Cause Exception e);

   @Message(value = "The '%s' JMX domain is already in use.", id = 34)
   JmxDomainConflictException jmxMBeanAlreadyRegistered(String jmxDomain, @Cause Throwable cause);

//   @LogMessage(level = WARN)
//   @Message(value = "Could not reflect field description of this class. Was it removed?", id = 35)
//   void couldNotFindDescriptionField();

   @LogMessage(level = WARN)
   @Message(value = "Did not find attribute %s", id = 36)
   void couldNotFindAttribute(String name);

   @LogMessage(level = WARN)
   @Message(value = "Failed to update attribute name %s with value %s", id = 37)
   void failedToUpdateAttribute(String name, Object value);

//   @LogMessage(level = WARN)
//   @Message(value = "Method name %s doesn't start with \"get\", \"set\", or \"is\" " +
//         "but is annotated with @ManagedAttribute: will be ignored", id = 38)
//   void ignoringManagedAttribute(String methodName);

//   @LogMessage(level = WARN)
//   @Message(value = "Method %s must have a valid return type and zero parameters", id = 39)
//   void invalidManagedAttributeMethod(String methodName);

//   @LogMessage(level = WARN)
//   @Message(value = "Not adding annotated method %s since we already have read attribute", id = 40)
//   void readManagedAttributeAlreadyPresent(Method m);

//   @LogMessage(level = WARN)
//   @Message(value = "Not adding annotated method %s since we already have writable attribute", id = 41)
//   void writeManagedAttributeAlreadyPresent(String methodName);

   @LogMessage(level = WARN)
   @Message(value = "Did not find queried attribute with name %s", id = 42)
   void queriedAttributeNotFound(String attributeName);

   @LogMessage(level = WARN)
   @Message(value = "Exception while writing value for attribute %s", id = 43)
   void errorWritingValueForAttribute(String attributeName, @Cause Exception e);

   @LogMessage(level = WARN)
   @Message(value = "Could not invoke set on attribute %s with value %s", id = 44)
   void couldNotInvokeSetOnAttribute(String attributeName, Object value);

//   @LogMessage(level = ERROR)
//   @Message(value = "Problems encountered while purging expired", id = 45)
//   void problemPurgingExpired(@Cause Exception e);

   @LogMessage(level = ERROR)
   @Message(value = "Unknown responses from remote cache: %s", id = 46)
   void unknownResponsesFromRemoteCache(Collection<Response> responses);

   @LogMessage(level = ERROR)
   @Message(value = "Error while doing remote call", id = 47)
   void errorDoingRemoteCall(@Cause Exception e);

//   @LogMessage(level = ERROR)
//   @Message(value = "Interrupted or timeout while waiting for AsyncCacheWriter worker threads to push all state to the decorated store", id = 48)
//   void interruptedWaitingAsyncStorePush(@Cause InterruptedException e);

//   @LogMessage(level = ERROR)
//   @Message(value = "Unexpected error", id = 51)
//   void unexpectedErrorInAsyncProcessor(@Cause Throwable t);

//   @LogMessage(level = ERROR)
//   @Message(value = "Interrupted on acquireLock for %d milliseconds!", id = 52)
//   void interruptedAcquiringLock(long ms, @Cause InterruptedException e);

//   @LogMessage(level = WARN)
//   @Message(value = "Unable to process some async modifications after %d retries!", id = 53)
//   void unableToProcessAsyncModifications(int retries);

//   @LogMessage(level = ERROR)
//   @Message(value = "Unexpected error in AsyncStoreCoordinator thread. AsyncCacheWriter is dead!", id = 55)
//   void unexpectedErrorInAsyncStoreCoordinator(@Cause Throwable t);

//   @SuppressWarnings("deprecation")
//   @LogMessage(level = ERROR)
//   @Message(value = "Exception reported changing cache active status", id = 58)
//   void errorChangingSingletonStoreStatus(@Cause SingletonCacheWriter.PushStateException e);

//   @LogMessage(level = WARN)
//   @Message(value = "Had problems removing file %s", id = 59)
//   void problemsRemovingFile(File f);

//   @LogMessage(level = WARN)
//   @Message(value = "Problems purging file %s", id = 60)
//   void problemsPurgingFile(File buckedFile, @Cause PersistenceException e);

//   @LogMessage(level = WARN)
//   @Message(value = "Unable to acquire global lock to purge cache store", id = 61)
//   void unableToAcquireLockToPurgeStore();

//   @LogMessage(level = ERROR)
//   @Message(value = "Error while reading from file: %s", id = 62)
//   void errorReadingFromFile(File f, @Cause Exception e);

//   @LogMessage(level = WARN)
//   @Message(value = "Problems creating the directory: %s", id = 64)
//   void problemsCreatingDirectory(File dir);

     @LogMessage(level = ERROR)
     @Message(value = "Exception while marshalling object: %s", id = 65)
     void errorMarshallingObject(@Cause Throwable ioe, Object obj);

//   @LogMessage(level = ERROR)
//   @Message(value = "Unable to read version id from first two bytes of stream, barfing.", id = 66)
//   void unableToReadVersionId();

//   @LogMessage(level = INFO)
//   @Message(value = "Will try and wait for the cache %s to start", id = 67)
//   void waitForCacheToStart(String cacheName);

//   @LogMessage(level = INFO)
//   @Message(value = "Cache named %s does not exist on this cache manager!", id = 68)
//   void namedCacheDoesNotExist(String cacheName);

   @LogMessage(level = WARN)
   @Message(value = "Caught exception when handling command %s", id = 71)
   void exceptionHandlingCommand(Object cmd, @Cause Throwable t);

   @LogMessage(level = ERROR)
   @Message(value = "Unexpected error while replicating", id = 73)
   void unexpectedErrorReplicating(@Cause Throwable t);

//   @LogMessage(level = ERROR)
//   @Message(value = "Message or message buffer is null or empty.", id = 77)
//   void msgOrMsgBufferEmpty();

   @LogMessage(level = INFO)
   @Message(value = "Starting JGroups channel `%s`", id = 78)
   void startingJGroupsChannel(String cluster);

   @LogMessage(level = INFO)
   @Message(value = "Starting JGroups channel `%s` with stack `%s`", id = 78)
   void startingJGroupsChannel(String cluster, String stack);

   @LogMessage(level = INFO)
   @Message(value = "Channel `%s` local address is `%s`, physical addresses are `%s`", id = 79)
   void localAndPhysicalAddress(String cluster, Address address, List<Address> physicalAddresses);

   @LogMessage(level = INFO)
   @Message(value = "Disconnecting JGroups channel `%s`", id = 80)
   void disconnectJGroups(String cluster);

   @LogMessage(level = ERROR)
   @Message(value = "Problem closing channel `%s`; setting it to null", id = 81)
   void problemClosingChannel(@Cause Exception e, String cluster);

//   @LogMessage(level = INFO)
//   @Message(value = "Stopping the RpcDispatcher for channel %s", id = 82)
//   void stoppingRpcDispatcher(String cluster);

   @LogMessage(level = ERROR)
   @Message(value = "Class [%s] cannot be cast to JGroupsChannelLookup! Not using a channel lookup.", id = 83)
   void wrongTypeForJGroupsChannelLookup(String channelLookupClassName, @Cause Exception e);

   @LogMessage(level = ERROR)
   @Message(value = "Errors instantiating [%s]! Not using a channel lookup.", id = 84)
   void errorInstantiatingJGroupsChannelLookup(String channelLookupClassName, @Cause Exception e);

   @Message(value = "Error while trying to create a channel using the specified configuration file: %s", id = 85)
   CacheConfigurationException errorCreatingChannelFromConfigFile(String cfg, @Cause Exception e);

   @Message(value = "Error while trying to create a channel using the specified configuration XML: %s", id = 86)
   CacheConfigurationException errorCreatingChannelFromXML(String cfg, @Cause Exception e);

   @Message(value = "Error while trying to create a channel using the specified configuration string: %s", id = 87)
   CacheConfigurationException errorCreatingChannelFromConfigString(String cfg, @Cause Exception e);

   @LogMessage(level = INFO)
   @Message(value = "Unable to use any JGroups configuration mechanisms provided in properties %s. " +
         "Using default JGroups configuration!", id = 88)
   void unableToUseJGroupsPropertiesProvided(TypedProperties props);

   @LogMessage(level = ERROR)
   @Message(value = "getCoordinator(): Interrupted while waiting for members to be set", id = 89)
   void interruptedWaitingForCoordinator(@Cause InterruptedException e);

//   @LogMessage(level = WARN)
//   @Message(value = "Channel not set up properly!", id = 92)
//   void channelNotSetUp();

   @LogMessage(level = INFO)
   @Message(value = "Received new, MERGED cluster view for channel %s: %s", id = 93)
   void receivedMergedView(String cluster, View newView);

   @LogMessage(level = INFO)
   @Message(value = "Received new cluster view for channel %s: %s", id = 94)
   void receivedClusterView(String cluster, View newView);

   @LogMessage(level = ERROR)
   @Message(value = "Error while processing a prepare in a single-phase transaction", id = 97)
   void errorProcessing1pcPrepareCommand(@Cause Throwable e);

   @LogMessage(level = ERROR)
   @Message(value = "Exception during rollback", id = 98)
   void errorRollingBack(@Cause Throwable e);

//   @LogMessage(level = ERROR)
//   @Message(value = "Unprocessed Transaction Log Entries! = %d", id = 99)
//   void unprocessedTxLogEntries(int size);

   @LogMessage(level = WARN)
   @Message(value = "Stopping, but there are %s local transactions and %s remote transactions that did not finish in time.", id = 100)
   void unfinishedTransactionsRemain(int localTransactions, int remoteTransactions);

   @LogMessage(level = WARN)
   @Message(value = "Failed synchronization registration", id = 101)
   void failedSynchronizationRegistration(@Cause Exception e);

   @LogMessage(level = WARN)
   @Message(value = "Unable to roll back global transaction %s", id = 102)
   void unableToRollbackGlobalTx(GlobalTransaction gtx, @Cause Throwable e);

//   @LogMessage(level = ERROR)
//   @Message(value = "A remote transaction with the given id was already registered!!!", id = 103)
//   void remoteTxAlreadyRegistered();

   @LogMessage(level = INFO)
   @Message(value = "Using EmbeddedTransactionManager", id = 104)
   void fallingBackToEmbeddedTm();

   @LogMessage(level = ERROR)
   @Message(value = "Failed creating initial JNDI context", id = 105)
   void failedToCreateInitialCtx(@Cause Throwable e);

//   @LogMessage(level = ERROR)
//   @Message(value = "Found WebSphere TransactionManager factory class [%s], but " +
//         "couldn't invoke its static 'getTransactionManager' method", id = 106)
//   void unableToInvokeWebsphereStaticGetTmMethod(@Cause Exception ex, String className);

   @LogMessage(level = INFO)
   @Message(value = "Retrieving transaction manager %s", id = 107)
   void retrievingTm(TransactionManager tm);

//   @LogMessage(level = ERROR)
//   @Message(value = "Error enlisting resource", id = 108)
//   void errorEnlistingResource(@Cause Throwable e);

//   @LogMessage(level = ERROR)
//   @Message(value = "beforeCompletion() failed for %s", id = 109)
//   void beforeCompletionFailed(Synchronization s, @Cause Throwable t);

//   @LogMessage(level = ERROR)
//   @Message(value = "Unexpected error from resource manager!", id = 110)
//   void unexpectedErrorFromResourceManager(@Cause Throwable t);

//   @LogMessage(level = ERROR)
//   @Message(value = "afterCompletion() failed for %s", id = 111)
//   void afterCompletionFailed(Synchronization s, @Cause Throwable t);

//   @LogMessage(level = WARN)
//   @Message(value = "exception while committing", id = 112)
//   void errorCommittingTx(@Cause Throwable e);

//   @LogMessage(level = ERROR)
//   @Message(value = "Unbinding of DummyTransactionManager failed", id = 113)
//   void unbindingDummyTmFailed(@Cause NamingException e);

//   @LogMessage(level = ERROR)
//   @Message(value = "Unsupported combination (dldEnabled, recoveryEnabled, xa) = (%s, %s, %s)", id = 114)
//   void unsupportedTransactionConfiguration(boolean dldEnabled, boolean recoveryEnabled, boolean xa);

   @LogMessage(level = WARN)
   @Message(value = "Recovery call will be ignored as recovery is disabled. " +
         "More on recovery: http://community.jboss.org/docs/DOC-16646", id = 115)
   void recoveryIgnored();

   @LogMessage(level = WARN)
   @Message(value = "Missing the list of prepared transactions from node %s. " +
         "Received response is %s", id = 116)
   void missingListPreparedTransactions(Object key, Object value);

   @LogMessage(level = ERROR)
   @Message(value = "There's already a prepared transaction with this xid: %s. " +
         "New transaction is %s. Are there two different transactions having same Xid in the cluster?", id = 117)
   void preparedTxAlreadyExists(RecoveryAwareTransaction previous,
                                RecoveryAwareRemoteTransaction remoteTransaction);

//   @LogMessage(level = WARN)
//   @Message(value = "Could not load module at URL %s", id = 118)
//   void couldNotLoadModuleAtUrl(URL url, @Cause Exception ex);

//   @LogMessage(level = WARN)
//   @Message(value = "Module %s loaded, but could not be initialized", id = 119)
//   void couldNotInitializeModule(Object key, @Cause Exception ex);

//   @LogMessage(level = WARN)
//   @Message(value = "Invocation of %s threw an exception %s. Exception is ignored.", id = 120)
//   void ignoringException(String methodName, String exceptionName, @Cause Throwable t);

   @LogMessage(level = ERROR)
   @Message(value = "Unable to set value!", id = 121)
   void unableToSetValue(@Cause Exception e);

//   @LogMessage(level = WARN)
//   @Message(value = "Unable to convert string property [%s] to an int! Using default value of %d", id = 122)
//   void unableToConvertStringPropertyToInt(String value, int defaultValue);

//   @LogMessage(level = WARN)
//   @Message(value = "Unable to convert string property [%s] to a long! Using default value of %d", id = 123)
//   void unableToConvertStringPropertyToLong(String value, long defaultValue);

//   @LogMessage(level = WARN)
//   @Message(value = "Unable to convert string property [%s] to a boolean! Using default value of %b", id = 124)
//   void unableToConvertStringPropertyToBoolean(String value, boolean defaultValue);

//   @LogMessage(level = WARN)
//   @Message(value = "Unable to invoke getter %s on Configuration.class!", id = 125)
//   void unableToInvokeGetterOnConfiguration(Method getter, @Cause Exception e);

   @LogMessage(level = WARN)
   @Message(value = "Attempted to stop() from FAILED state, but caught exception", id = 126)
   void failedToCallStopAfterFailure(@Cause Throwable t);

//   @LogMessage(level = WARN)
//   @Message(value = "Needed to call stop() before destroying but stop() threw exception. Proceeding to destroy", id = 127)
//   void stopBeforeDestroyFailed(@Cause CacheException e);

   @LogMessage(level = DEBUG)
   @Message(value = "Infinispan version: %s", id = 128)
   void version(String version);

//   @LogMessage(level = WARN)
//   @Message(value = "Received a remote call but the cache is not in STARTED state - ignoring call.", id = 129)
//   void cacheNotStarted();

//   @LogMessage(level = ERROR)
//   @Message(value = "Caught exception! Aborting join.", id = 130)
//   void abortingJoin(@Cause Exception e);

//   @LogMessage(level = INFO)
//   @Message(value = "%s completed join rehash in %s!", id = 131)
//   void joinRehashCompleted(Address self, String duration);

//   @LogMessage(level = INFO)
//   @Message(value = "%s aborted join rehash after %s!", id = 132)
//   void joinRehashAborted(Address self, String duration);

   @LogMessage(level = WARN)
   @Message(value = "Attempted to register listener of class %s, but no valid, " +
         "public methods annotated with method-level event annotations found! " +
         "Ignoring listener.", id = 133)
   void noAnnotateMethodsFoundInListener(Class<?> listenerClass);

   @LogMessage(level = WARN)
   @Message(value = "Unable to invoke method %s on Object instance %s - " +
         "removing this target object from list of listeners!", id = 134)
   void unableToInvokeListenerMethodAndRemoveListener(Method m, Object target, @Cause Throwable e);

   @LogMessage(level = WARN)
   @Message(value = "Could not lock key %s in order to invalidate from L1 at node %s, skipping....", id = 135)
   void unableToLockToInvalidate(Object key, Address address);

   @LogMessage(level = ERROR)
   @Message(value = "Error executing command %s on %s, writing keys %s", id = 136)
   void executionError(String commandType, String cacheName, String affectedKeys, @Cause Throwable t);

   @LogMessage(level = INFO)
   @Message(value = "Failed invalidating remote cache", id = 137)
   void failedInvalidatingRemoteCache(@Cause Throwable e);

//   @LogMessage(level = INFO)
//   @Message(value = "Could not register object with name: %s", id = 138)
//   void couldNotRegisterObjectName(Object objectName, @Cause Throwable e);

//   @LogMessage(level = WARN)
//   @Message(value = "Infinispan configuration schema could not be resolved locally nor fetched from URL. Local path=%s, schema path=%s, schema URL=%s", id = 139)
//   void couldNotResolveConfigurationSchema(String localPath, String schemaPath, String schemaURL);

//   @LogMessage(level = WARN)
//   @Message(value = "Lazy deserialization configuration is deprecated, please use storeAsBinary instead", id = 140)
//   void lazyDeserializationDeprecated();

   @LogMessage(level = WARN)
   @Message(value = "Could not rollback prepared 1PC transaction. This transaction will be rolled back by the recovery process, if enabled. Transaction: %s", id = 141)
   void couldNotRollbackPrepared1PcTransaction(LocalTransaction localTransaction, @Cause Throwable e1);

//   @LogMessage(level = WARN)
//   @Message(value = "Received a key that doesn't map to this node: %s, mapped to %s", id = 143)
//   void keyDoesNotMapToLocalNode(Object key, Collection<Address> nodes);

//   @LogMessage(level = WARN)
//   @Message(value = "Failed loading value for key %s from cache store", id = 144)
//   void failedLoadingValueFromCacheStore(Object key, @Cause Exception e);

   @LogMessage(level = ERROR)
   @Message(value = "Error invalidating keys from L1 after rehash", id = 147)
   void failedToInvalidateKeys(@Cause Throwable e);

//   @LogMessage(level = WARN)
//   @Message(value = "Invalid %s value of %s. It can not be higher than %s which is %s", id = 148)
//   void invalidTimeoutValue(Object configName1, Object value1, Object configName2, Object value2);

//   @LogMessage(level = WARN)
//   @Message(value = "Fetch persistent state and purge on startup are both disabled, cache may contain stale entries on startup", id = 149)
//   void staleEntriesWithoutFetchPersistentStateOrPurgeOnStartup();

//   @LogMessage(level = FATAL)
//   @Message(value = "Rehash command received on non-distributed cache. All the nodes in the cluster should be using the same configuration.", id = 150)
//   void rehashCommandReceivedOnNonDistributedCache();

//   @LogMessage(level = ERROR)
//   @Message(value = "Error flushing to file: %s", id = 151)
//   void errorFlushingToFileChannel(FileChannel f, @Cause Exception e);

   @LogMessage(level = INFO)
   @Message(value = "Passivation configured without an eviction policy being selected. " +
         "Only manually evicted entities will be passivated.", id = 152)
   void passivationWithoutEviction();

   // Warning ISPN000153 removed as per ISPN-2554

//   @LogMessage(level = ERROR)
//   @Message(value = "Unable to unlock keys %2$s for transaction %1$s after they were rebalanced off node %3$s", id = 154)
//   void unableToUnlockRebalancedKeys(GlobalTransaction gtx, List<Object> keys, Address self, @Cause Throwable t);

//   @LogMessage(level = WARN)
//   @Message(value = "Unblocking transactions failed", id = 159)
//   void errorUnblockingTransactions(@Cause Exception e);

   @LogMessage(level = WARN)
   @Message(value = "Could not complete injected transaction.", id = 160)
   void couldNotCompleteInjectedTransaction(@Cause Throwable t);

   @LogMessage(level = INFO)
   @Message(value = "Using a batchMode transaction manager", id = 161)
   void usingBatchModeTransactionManager();

   @LogMessage(level = INFO)
   @Message(value = "Could not instantiate transaction manager", id = 162)
   void couldNotInstantiateTransactionManager(@Cause Exception e);

//   @LogMessage(level = WARN)
//   @Message(value = "FileCacheStore ignored an unexpected file %2$s in path %1$s. The store path should be dedicated!", id = 163)
//   void cacheLoaderIgnoringUnexpectedFile(Object parentPath, String name);

//   @LogMessage(level = ERROR)
//   @Message(value = "Rolling back to cache view %d, but last committed view is %d", id = 164)
//   void cacheViewRollbackIdMismatch(int committedViewId, int committedView);

//   @LogMessage(level = INFO)
//   @Message(value = "Strict peer-to-peer is enabled but the JGroups channel was started externally - this is very likely to result in RPC timeout errors on startup", id = 171)
//   void warnStrictPeerToPeerWithInjectedChannel();

//   @LogMessage(level = ERROR)
//   @Message(value = "Custom interceptor %s has used @Inject, @Start or @Stop. These methods will not be processed. Please extend org.infinispan.interceptors.base.BaseCustomInterceptor instead, and your custom interceptor will have access to a cache and cacheManager.  Override stop() and start() for lifecycle methods.", id = 173)
//   void customInterceptorExpectsInjection(String customInterceptorFQCN);

//   @LogMessage(level = WARN)
//   @Message(value = "Unexpected error reading configuration", id = 174)
//   void errorReadingConfiguration(@Cause Exception e);

//   @LogMessage(level = WARN)
//   @Message(value = "Unexpected error closing resource", id = 175)
//   void failedToCloseResource(@Cause Throwable e);

//   @LogMessage(level = WARN)
//   @Message(value = "The 'wakeUpInterval' attribute of the 'eviction' configuration XML element is deprecated. Setting the 'wakeUpInterval' attribute of the 'expiration' configuration XML element to %d instead", id = 176)
//   void evictionWakeUpIntervalDeprecated(Long wakeUpInterval);

   @LogMessage(level = WARN)
   @Message(value = "%s has been deprecated as a synonym for %s. Use one of %s instead", id = 177)
   void randomCacheModeSynonymsDeprecated(String candidate, String mode, List<String> synonyms);

//   @LogMessage(level = WARN)
//   @Message(value = "stateRetrieval's 'alwaysProvideInMemoryState' attribute is no longer in use, " +
//         "instead please make sure all instances of this named cache in the cluster have 'fetchInMemoryState' attribute enabled", id = 178)
//   void alwaysProvideInMemoryStateDeprecated();

//   @LogMessage(level = WARN)
//   @Message(value = "stateRetrieval's 'initialRetryWaitTime' attribute is no longer in use.", id = 179)
//   void initialRetryWaitTimeDeprecated();

//   @LogMessage(level = WARN)
//   @Message(value = "stateRetrieval's 'logFlushTimeout' attribute is no longer in use.", id = 180)
//   void logFlushTimeoutDeprecated();

//   @LogMessage(level = WARN)
//   @Message(value = "stateRetrieval's 'maxProgressingLogWrites' attribute is no longer in use.", id = 181)
//   void maxProgressingLogWritesDeprecated();

//   @LogMessage(level = WARN)
//   @Message(value = "stateRetrieval's 'numRetries' attribute is no longer in use.", id = 182)
//   void numRetriesDeprecated();

//   @LogMessage(level = WARN)
//   @Message(value = "stateRetrieval's 'retryWaitTimeIncreaseFactor' attribute is no longer in use.", id = 183)
//   void retryWaitTimeIncreaseFactorDeprecated();

//   @LogMessage(level = INFO)
//   @Message(value = "The stateRetrieval configuration element has been deprecated, " +
//         "we're assuming you meant stateTransfer. Please see XML schema for more information.", id = 184)
//   void stateRetrievalConfigurationDeprecated();

//   @LogMessage(level = INFO)
//   @Message(value = "hash's 'rehashEnabled' attribute has been deprecated. Please use stateTransfer.fetchInMemoryState instead", id = 185)
//   void hashRehashEnabledDeprecated();

//   @LogMessage(level = INFO)
//   @Message(value = "hash's 'rehashRpcTimeout' attribute has been deprecated. Please use stateTransfer.timeout instead", id = 186)
//   void hashRehashRpcTimeoutDeprecated();

//   @LogMessage(level = INFO)
//   @Message(value = "hash's 'rehashWait' attribute has been deprecated. Please use stateTransfer.timeout instead", id = 187)
//   void hashRehashWaitDeprecated();

   @LogMessage(level = ERROR)
   @Message(value = "Error while processing a commit in a two-phase transaction", id = 188)
   void errorProcessing2pcCommitCommand(@Cause Throwable e);

   @LogMessage(level = WARN)
   @Message(value = "While stopping a cache or cache manager, one of its components failed to stop", id = 189)
   void componentFailedToStop(@Cause Throwable e);

//   @LogMessage(level = WARN)
//   @Message(value = "Use of the 'loader' element to configure a store is deprecated, please use the 'store' element instead", id = 190)
//   void deprecatedLoaderAsStoreConfiguration();

//   @LogMessage(level = DEBUG)
//   @Message(value = "When indexing locally a cache with shared cache loader, preload must be enabled", id = 191)
//   void localIndexingWithSharedCacheLoaderRequiresPreload();

//   @LogMessage(level = WARN)
//   @Message(value = "hash's 'numVirtualNodes' attribute has been deprecated. Please use hash.numSegments instead", id = 192)
//   void hashNumVirtualNodesDeprecated();

//   @LogMessage(level = WARN)
//   @Message(value = "hash's 'consistentHash' attribute has been deprecated. Please use hash.consistentHashFactory instead", id = 193)
//   void consistentHashDeprecated();

   @LogMessage(level = WARN)
   @Message(value = "Failed loading keys from cache store", id = 194)
   void failedLoadingKeysFromCacheStore(@Cause Throwable t);

   @LogMessage(level = ERROR)
   @Message(value = "Error during rebalance for cache %s on node %s, topology id = %d", id = 195)
   void rebalanceError(String cacheName, Address node, int topologyId, @Cause Throwable cause);

   @LogMessage(level = WARN)
   @Message(value = "Failed to recover cluster state after the current node became the coordinator (or after merge), will retry", id = 196)
   void failedToRecoverClusterState(@Cause Throwable cause);

   @LogMessage(level = WARN)
   @Message(value = "Error updating cluster member list for view %d, waiting for next view", id = 197)
   void errorUpdatingMembersList(int viewId, @Cause Throwable cause);

//   @LogMessage(level = INFO)
//   @Message(value = "Unable to register MBeans for default cache", id = 198)
//   void unableToRegisterMBeans();

//   @LogMessage(level = INFO)
//   @Message(value = "Unable to register MBeans for named cache %s", id = 199)
//   void unableToRegisterMBeans(String cacheName);

//   @LogMessage(level = INFO)
//   @Message(value = "Unable to register MBeans for cache manager", id = 200)
//   void unableToRegisterCacheManagerMBeans();

   @LogMessage(level = TRACE)
   @Message(value = "This cache is configured to backup to its own site (%s).", id = 201)
   void cacheBackupsDataToSameSite(String siteName);

   @LogMessage(level = WARN)
   @Message(value = "Encountered issues while backing up data for cache %s to site %s", id = 202)
   @Description("This message indicates an issue has occurred with state transfer operations. First check that the site is online and if any network issues have occurred. Confirm that the relay nodes in the cluster are not overloaded with cross-site replication requests. In some cases garbage collection pauses can also interrupt backup operations. You can either increase the amount of memory available to relay nodes or increase the number of relay nodes in the cluster.")
   void warnXsiteBackupFailed(String cacheName, String siteName, @Cause Throwable throwable);

   @LogMessage(level = WARN)
   @Message(value = "The rollback request for tx %s cannot be processed by the cache %s as this cache is not transactional!", id = 203)
   void cannotRespondToRollback(GlobalTransaction globalTransaction, String cacheName);

   @LogMessage(level = WARN)
   @Message(value = "The commit request for tx %s cannot be processed by the cache %s as this cache is not transactional!", id = 204)
   void cannotRespondToCommit(GlobalTransaction globalTransaction, String cacheName);

   @LogMessage(level = WARN)
   @Message(value = "Trying to bring back an non-existent site (%s)!", id = 205)
   void tryingToBringOnlineNonexistentSite(String siteName);

//   @LogMessage(level = WARN)
//   @Message(value = "Could not execute cancellation command locally", id = 206)
//   void couldNotExecuteCancellationLocally(@Cause Throwable e);

//   @LogMessage(level = WARN)
//   @Message(value = "Could not interrupt as no thread found for command uuid %s", id = 207)
//   void couldNotInterruptThread(UUID id);

   @LogMessage(level = ERROR)
   @Message(value = "No live owners found for segments %s of cache %s. Excluded owners: %s", id = 208)
   void noLiveOwnersFoundForSegments(Collection<Integer> segments, String cacheName, Collection<Address> faultySources);

   @LogMessage(level = WARN)
   @Message(value = "Failed to retrieve transactions of cache %s from node %s, segments %s", id = 209)
   void failedToRetrieveTransactionsForSegments(String cacheName, Address source, Collection<Integer> segments, @Cause Throwable t);

   @LogMessage(level = WARN)
   @Message(value = "Failed to request state of cache %s from node %s, segments %s", id = 210)
   void failedToRequestSegments(String cacheName, Address source, Collection<Integer> segments, @Cause Throwable e);

//   @LogMessage(level = ERROR)
//   @Message(value = "Unable to load %s from any of the following classloaders: %s", id=213)
//   void unableToLoadClass(String classname, String classloaders, @Cause Throwable cause);

   @LogMessage(level = WARN)
   @Message(value = "Unable to remove entry under %s from cache store after activation", id = 214)
   void unableToRemoveEntryAfterActivation(Object key, @Cause Throwable e);

   @Message(value = "Unknown migrator %s", id = 215)
   IllegalArgumentException unknownMigrator(String migratorName);

   @LogMessage(level = INFO)
   @Message(value = "%d entries migrated to cache %s in %s", id = 216)
   void entriesMigrated(long count, String name, String prettyTime);

   @Message(value = "Received exception from %s, see cause for remote stack trace", id = 217)
   RemoteException remoteException(Address sender, @Cause Throwable t);

//   @LogMessage(level = INFO)
//   @Message(value = "Timeout while waiting for the transaction validation. The command will not be processed. " +
//         "Transaction is %s", id = 218)
//   void timeoutWaitingUntilTransactionPrepared(String globalTx);

//   @LogMessage(level = WARN)
//   @Message(value = "Problems un-marshalling remote command from byte buffer", id = 220)
//   void errorUnMarshallingCommand(@Cause Throwable throwable);

   //@LogMessage(level = WARN)
   //@Message(value = "Unknown response value [%s]. Expected [%s]", id = 221)
   //void unexpectedResponse(String actual, String expected);

   @Message(value = "Custom interceptor missing class", id = 222)
   CacheConfigurationException customInterceptorMissingClass();

   @LogMessage(level = WARN)
   @Message(value = "Custom interceptor '%s' does not extend BaseCustomInterceptor, which is recommended", id = 223)
   void suggestCustomInterceptorInheritance(String customInterceptorClassName);

   @Message(value = "Custom interceptor '%s' specifies more than one position", id = 224)
   CacheConfigurationException multipleCustomInterceptorPositions(String customInterceptorClassName);

   @Message(value = "Custom interceptor '%s' doesn't specify a position", id = 225)
   CacheConfigurationException missingCustomInterceptorPosition(String customInterceptorClassName);

//   @Message(value = "Error while initializing SSL context", id = 226)
//   CacheConfigurationException sslInitializationException(@Cause Throwable e);

//   @LogMessage(level = WARN)
//   @Message(value = "Support for concurrent updates can no longer be configured (it is always enabled by default)", id = 227)
//   void warnConcurrentUpdateSupportCannotBeConfigured();

   @LogMessage(level = ERROR)
   @Message(value = "Failed to recover cache %s state after the current node became the coordinator", id = 228)
   void failedToRecoverCacheState(String cacheName, @Cause Throwable cause);

   @Message(value = "Unexpected initial version type (only NumericVersion instances supported): %s", id = 229)
   IllegalArgumentException unexpectedInitialVersion(String className);

   @LogMessage(level = ERROR)
   @Message(value = "Failed to start rebalance for cache %s", id = 230)
   void rebalanceStartError(String cacheName, @Cause Throwable cause);

//   @Message(value="Cache mode should be DIST or REPL, rather than %s", id = 231)
//   IllegalStateException requireDistOrReplCache(String cacheType);

//   @Message(value="Cache is in an invalid state: %s", id = 232)
//   IllegalStateException invalidCacheState(String cacheState);

   @LogMessage(level = WARN)
   @Message(value = "Root element for %s already registered in ParserRegistry by %s. Cannot install %s.", id = 234)
   void parserRootElementAlreadyRegistered(String qName, String oldParser, String newParser);

   @Message(value = "Configuration parser %s does not declare any Namespace annotations", id = 235)
   CacheConfigurationException parserDoesNotDeclareNamespaces(String name);

//   @Message(value = "Purging expired entries failed", id = 236)
//   PersistenceException purgingExpiredEntriesFailed(@Cause Throwable cause);

//   @Message(value = "Waiting for expired entries to be purge timed out", id = 237)
//   PersistenceException timedOutWaitingForExpiredEntriesToBePurged(@Cause Throwable cause);

   @Message(value = "Directory %s does not exist and cannot be created!", id = 238)
   CacheConfigurationException directoryCannotBeCreated(String path);

//   @Message(value="Cache manager is shutting down, so type write externalizer for type=%s cannot be resolved", id = 239)
//   IOException externalizerTableStopped(String className);

//   @Message(value="Cache manager is shutting down, so type (id=%d) cannot be resolved. Interruption being pushed up.", id = 240)
//   IOException pushReadInterruptionDueToCacheManagerShutdown(int readerIndex, @Cause InterruptedException cause);

//   @Message(value="Cache manager is %s and type (id=%d) cannot be resolved (thread not interrupted)", id = 241)
//   CacheException cannotResolveExternalizerReader(ComponentStatus status, int readerIndex);

   @Message(value = "Missing foreign externalizer with id=%s, either externalizer was not configured by client, or module lifecycle implementation adding externalizer was not loaded properly", id = 242)
   CacheException missingForeignExternalizer(int foreignId);

   @Message(value = "Type of data read is unknown. Id=%d is not amongst known reader indexes.", id = 243)
   CacheException unknownExternalizerReaderIndex(int readerIndex);

   @Message(value = "AdvancedExternalizer's getTypeClasses for externalizer %s must return a non-empty set", id = 244)
   CacheConfigurationException advanceExternalizerTypeClassesUndefined(String className);

   @Message(value = "Duplicate id found! AdvancedExternalizer id=%d for %s is shared by another externalizer (%s). Reader index is %d", id = 245)
   CacheConfigurationException duplicateExternalizerIdFound(int externalizerId, Class<?> typeClass, String otherExternalizer, int readerIndex);

//   @Message(value = "Internal %s externalizer is using an id(%d) that exceeded the limit. It needs to be smaller than %d", id = 246)
//   CacheConfigurationException internalExternalizerIdLimitExceeded(AdvancedExternalizer<?> ext, int externalizerId, int maxId);
//
//   @Message(value = "Foreign %s externalizer is using a negative id(%d). Only positive id values are allowed.", id = 247)
//   CacheConfigurationException foreignExternalizerUsingNegativeId(AdvancedExternalizer<?> ext, int externalizerId);

//   @Message(value =  "Invalid cache loader configuration!!  Only ONE cache loader may have fetchPersistentState set " +
//         "to true.  Cache will not start!", id = 248)
//   CacheConfigurationException multipleCacheStoresWithFetchPersistentState();

   @Message(value = "The cache loader configuration %s does not specify the loader class using @ConfigurationFor", id = 249)
   CacheConfigurationException loaderConfigurationDoesNotSpecifyLoaderClass(String className);

//   @Message(value = "Invalid configuration, expecting '%s' got '%s' instead", id = 250)
//   CacheConfigurationException incompatibleLoaderConfiguration(String expected, String actual);

//   @Message(value = "Cache Loader configuration cannot be null", id = 251)
//   CacheConfigurationException cacheLoaderConfigurationCannotBeNull();

//   @LogMessage(level = ERROR)
//   @Message(value = "Error executing parallel store task", id = 252)
//   void errorExecutingParallelStoreTask(@Cause Throwable cause);

//   @Message(value = "Invalid Cache Loader class: %s", id = 253)
//   CacheConfigurationException invalidCacheLoaderClass(String name);

//   @LogMessage(level = WARN)
//   @Message(value = "The transport element's 'strictPeerToPeer' attribute is no longer in use.", id = 254)
//   void strictPeerToPeerDeprecated();

   @LogMessage(level = ERROR)
   @Message(value = "Error while processing prepare", id = 255)
   void errorProcessingPrepare(@Cause Throwable e);

//   @LogMessage(level = ERROR)
//   @Message(value = "Configurator SAXParse error", id = 256)
//   void configuratorSAXParseError(@Cause Exception e);
//
//   @LogMessage(level = ERROR)
//   @Message(value = "Configurator SAX error", id = 257)
//   void configuratorSAXError(@Cause Exception e);
//
//   @LogMessage(level = ERROR)
//   @Message(value = "Configurator general error", id = 258)
//   void configuratorError(@Cause Exception e);

//   @LogMessage(level = ERROR)
//   @Message(value = "Async store executor did not stop properly", id = 259)
//   void errorAsyncStoreNotStopped();

//   @LogMessage(level = ERROR)
//   @Message(value = "Exception executing command", id = 260)
//   void exceptionExecutingInboundCommand(@Cause Exception e);

   @LogMessage(level = ERROR)
   @Message(value = "Failed to execute outbound transfer", id = 261)
   void failedOutBoundTransferExecution(@Cause Throwable e);

   @LogMessage(level = ERROR)
   @Message(value = "Failed to enlist TransactionXaAdapter to transaction", id = 262)
   void failedToEnlistTransactionXaAdapter(@Cause Throwable e);

//   @LogMessage(level = WARN)
//   @Message(value = "FIFO strategy is deprecated, LRU will be used instead", id = 263)
//   void warnFifoStrategyIsDeprecated();

   @LogMessage(level = WARN)
   @Message(value = "Not using an L1 invalidation reaper thread. This could lead to memory leaks as the requestors map may grow indefinitely!", id = 264)
   void warnL1NotHavingReaperThread();

   @LogMessage(level = WARN)
   @Message(value = "Problems creating interceptor %s", id = 267)
   void unableToCreateInterceptor(Class<?> type, @Cause Exception e);

   @LogMessage(level = WARN)
   @Message(value = "Unable to broadcast invalidations as a part of the prepare phase. Rolling back.", id = 268)
   void unableToRollbackInvalidationsDuringPrepare(@Cause Throwable e);

   @LogMessage(level = WARN)
   @Message(value = "Cache used for Grid metadata should be synchronous.", id = 269)
   void warnGridFSMetadataCacheRequiresSync();

   @LogMessage(level = WARN)
   @Message(value = "Could not commit local tx %s", id = 270)
   void warnCouldNotCommitLocalTx(Object transactionDescription, @Cause Throwable e);

   @LogMessage(level = WARN)
   @Message(value = "Could not rollback local tx %s", id = 271)
   void warnCouldNotRollbackLocalTx(Object transactionDescription, @Cause Throwable e);

   @LogMessage(level = WARN)
   @Message(value = "Exception removing recovery information", id = 272)
   void warnExceptionRemovingRecovery(@Cause Throwable e);

   @Message(value = "Indexing can not be enabled on caches in Invalidation mode", id = 273)
   CacheConfigurationException invalidConfigurationIndexingWithInvalidation();

   @LogMessage(level = ERROR)
   @Message(value = "Persistence enabled without any CacheLoaderInterceptor in InterceptorChain!", id = 274)
   void persistenceWithoutCacheLoaderInterceptor();

   @LogMessage(level = ERROR)
   @Message(value = "Persistence enabled without any CacheWriteInterceptor in InterceptorChain!", id = 275)
   void persistenceWithoutCacheWriteInterceptor();

//   @Message(value = "Could not find migration data in cache %s", id = 276)
//   CacheException missingMigrationData(String name);

//   @LogMessage(level = WARN)
//   @Message(value = "Could not migrate key %s", id = 277)
//   void keyMigrationFailed(String key, @Cause Throwable cause);

   @Message(value = "Indexing can only be enabled if infinispan-query.jar is available on your classpath, and this jar has not been detected.", id = 278)
   CacheConfigurationException invalidConfigurationIndexingWithoutModule();

   @Message(value = "Failed to read stored entries from file. Error in file %s at offset %d", id = 279)
   PersistenceException errorReadingFileStore(String path, long offset);

   @Message(value = "Caught exception [%s] while invoking method [%s] on listener instance: %s", id = 280)
   CacheListenerException exceptionInvokingListener(String name, Method m, Object target, @Cause Throwable cause);

   @Message(value = "%s reported that a third node was suspected, see cause for info on the node that was suspected", id = 281)
   SuspectException thirdPartySuspected(Address sender, @Cause SuspectException e);

   @Message(value = "Cannot enable Invocation Batching when the Transaction Mode is NON_TRANSACTIONAL, set the transaction mode to TRANSACTIONAL", id = 282)
   CacheConfigurationException invocationBatchingNeedsTransactionalCache();

   @Message(value = "A cache configured with invocation batching can't have recovery enabled", id = 283)
   CacheConfigurationException invocationBatchingCannotBeRecoverable();

   @LogMessage(level = WARN)
   @Message(value = "Problem encountered while installing cluster listener", id = 284)
   void clusterListenerInstallationFailure(@Cause Throwable cause);

   @LogMessage(level = WARN)
   @Message(value = "Issue when retrieving cluster listeners from %s response was %s", id = 285)
   void unsuccessfulResponseForClusterListeners(Address address, Response response);

   @LogMessage(level = WARN)
   @Message(value = "Issue when retrieving cluster listeners from %s", id = 286)
   void exceptionDuringClusterListenerRetrieval(Address address, @Cause Throwable cause);

   @Message(value = "Unauthorized access: subject '%s' lacks '%s' permission", id = 287)
   SecurityException unauthorizedAccess(String subject, String permission);

   @Message(value = "A principal-to-role mapper has not been specified", id = 288)
   CacheConfigurationException invalidPrincipalRoleMapper();

   @LogMessage(level = WARN)
   @Message(value = "Cannot send cross-site state chunk to '%s'.", id = 289)
   @Description("During a state transfer operation it was not possible to transfer a batch of cache entries. First check that the site is online and if any network issues have occurred. Confirm that the relay nodes in the cluster are not overloaded with cross-site replication requests. In some cases garbage collection pauses can also interrupt backup operations. You can either increase the amount of memory available to relay nodes or increase the number of relay nodes in the cluster.")
   void unableToSendXSiteState(String site, @Cause Throwable cause);

//   @LogMessage(level = WARN)
//   @Message(value = "Unable to wait for X-Site state chunk ACKs from '%s'.", id = 290)
//   void unableToWaitForXSiteStateAcks(String site, @Cause Throwable cause);

   @LogMessage(level = WARN)
   @Message(value = "Cannot apply cross-site state chunk.", id = 291)
   @Description("During a state transfer operation it was not possible to apply a batch of cache entries. Ensure that sites are online and check network status.")
   void unableToApplyXSiteState(@Cause Throwable cause);

//   @LogMessage(level = WARN)
//   @Message(value = "Unrecognized attribute '%s'. Please check your configuration. Ignoring!", id = 292)
//   void unrecognizedAttribute(String property);

   @LogMessage(level = INFO)
   @Message(value = "Ignoring attribute '%2$s' of element '%1$s' at '%3$s', please remove from configuration file", id = 293)
   void ignoreAttribute(String element, Object attribute, Location location);

   @LogMessage(level = INFO)
   @Message(value = "Ignoring element %s at %s, please remove from configuration file", id = 294)
   void ignoreXmlElement(Object element, Location location);

   @Message(value = "No thread pool with name '%s' found", id = 295)
   CacheConfigurationException undefinedThreadPoolName(String name);

   @Message(value = "Attempt to add a %s permission to a SecurityPermissionCollection", id = 296)
   IllegalArgumentException invalidPermission(Permission permission);

   @Message(value = "Attempt to add a permission to a read-only SecurityPermissionCollection", id = 297)
   SecurityException readOnlyPermissionCollection();

//   @LogMessage(level = DEBUG)
//   @Message(value = "Using internal security checker", id = 298)
//   void authorizationEnabledWithoutSecurityManager();

   @Message(value = "Unable to acquire lock after %s for key %s and requestor %s. Lock is held by %s", id = 299)
   TimeoutException unableToAcquireLock(String timeout, Object key, Object requestor, Object owner);

   @Message(value = "There was an exception while processing retrieval of entry values", id = 300)
   CacheException exceptionProcessingEntryRetrievalValues(@Cause Throwable cause);

//   @Message(value = "Iterator response for identifier %s encountered unexpected exception", id = 301)
//   CacheException exceptionProcessingIteratorResponse(UUID identifier, @Cause Throwable cause);

   @LogMessage(level = WARN)
   @Message(value = "Issue when retrieving transactions from %s, response was %s", id = 302)
   void unsuccessfulResponseRetrievingTransactionsForSegments(Address address, Response response);

   @LogMessage(level = WARN)
   @Message(value = "More than one configuration file with specified name on classpath. The first one will be used:\n %s", id = 304)
   void ambiguousConfigurationFiles(String files);

   @Message(value = "Cluster is operating in degraded mode because of node failures.", id = 305)
   AvailabilityException partitionDegraded();

   @Message(value = "Key '%s' is not available. Not all owners are in this partition", id = 306)
   AvailabilityException degradedModeKeyUnavailable(Object key);

   @Message(value = "Cannot clear when the cluster is partitioned", id = 307)
   AvailabilityException clearDisallowedWhilePartitioned();

   @LogMessage(level = INFO)
   @Message(value = "Rebalancing enabled", id = 308)
   void rebalancingEnabled();

   @LogMessage(level = INFO)
   @Message(value = "Rebalancing suspended", id = 309)
   void rebalancingSuspended();

   @LogMessage(level = DEBUG)
   @Message(value = "Starting new rebalance phase for cache %s, topology %s", id = 310)
   void startingRebalancePhase(String cacheName, CacheTopology cacheTopology);

   // Messages between 312 and 320 have been moved to the org.infinispan.util.logging.events.Messages class

   @LogMessage(level = WARN)
   @Message(value = "Cyclic dependency detected between caches, stop order ignored", id = 321)
   void stopOrderIgnored();

   @LogMessage(level = WARN)
   @Message(value = "Cannot restart cross-site state transfer to site %s", id = 322)
   @Description("It was not possible to resume a state transfer operation to a backup location. Ensure that sites are online and check network status.")
   void failedToRestartXSiteStateTransfer(String siteName, @Cause Throwable cause);

   @Message(value = "%s is in '%s' state and so it does not accept new invocations. " +
         "Either restart it or recreate the cache container.", id = 323)
   IllegalLifecycleStateException cacheIsTerminated(String cacheName, String state);

   @Message(value = "%s is in 'STOPPING' state and this is an invocation not belonging to an on-going transaction, so it does not accept new invocations. " +
         "Either restart it or recreate the cache container.", id = 324)
   IllegalLifecycleStateException cacheIsStopping(String cacheName);

//   @Message(value = "Creating tmp cache %s timed out waiting for rebalancing to complete on node %s ", id = 325)
//   RuntimeException creatingTmpCacheTimedOut(String cacheName, Address address);

   @LogMessage(level = WARN)
   @Message(value = "Remote transaction %s timed out. Rolling back after %d ms", id = 326)
   void remoteTransactionTimeout(GlobalTransaction gtx, long ageMilliSeconds);

   @Message(value = "Cannot find a parser for element '%s' in namespace '%s' at %s. Check that your configuration is up-to-date for Infinispan '%s' and you have the proper dependency in the classpath", id = 327)
   CacheConfigurationException unsupportedConfiguration(String element, String namespaceUri, Location location, String version);

   @LogMessage(level = DEBUG)
   @Message(value = "Rebalance phase %s confirmed for cache %s on node %s, topology id = %d", id = 328)
   void rebalancePhaseConfirmedOnNode(CacheTopology.Phase phase, String cacheName, Address node, int topologyId);

   @LogMessage(level = WARN)
   @Message(value = "Unable to read rebalancing status from coordinator %s", id = 329)
   void errorReadingRebalancingStatus(Address coordinator, @Cause Throwable t);

//   @LogMessage(level = WARN)
//   @Message(value = "Distributed task failed at %s. The task is failing over to be executed at %s", id = 330)
//   void distributedTaskFailover(Address failedAtAddress, Address failoverTarget, @Cause Exception e);

   @LogMessage(level = WARN)
   @Message(value = "Unable to invoke method %s on Object instance %s ", id = 331)
   void unableToInvokeListenerMethod(Method m, Object target, @Cause Throwable e);

//   @Message(value = "Remote transaction %s rolled back because originator is no longer in the cluster", id = 332)
//   CacheException orphanTransactionRolledBack(GlobalTransaction gtx);

//   @Message(value = "The site must be specified.", id = 333)
//   CacheConfigurationException backupSiteNullName();

//   @Message(value = "Using a custom failure policy requires a failure policy class to be specified.", id = 334)
//   CacheConfigurationException customBackupFailurePolicyClassNotSpecified();

   @Message(value = "Two-phase commit can only be used with synchronous backup strategy.", id = 335)
   CacheConfigurationException twoPhaseCommitAsyncBackup();

   @LogMessage(level = DEBUG)
   @Message(value = "Finished rebalance for cache %s, topology %s", id = 336)
   void finishedRebalance(String cacheName, CacheTopology topology);

   @Message(value = "Backup configuration must include a 'site'.", id = 337)
   @Description("Caches that use cross-site replication must include a site in the configuration. Edit the cache and specify the name of the site in the backup configuration.")
   CacheConfigurationException backupMissingSite();

   @Message(value = "You must specify a 'failure-policy-class' to use a custom backup failure policy for backup '%s'.", id = 338)
   @Description("The backup configuration for the cache uses a custom failure policy but does not include the fully qualified class of a custom failure policy implementation. Specify the failure policy class in the backup configuration or use a different failure policy.")
   CacheConfigurationException missingBackupFailurePolicyClass(String remoteSite);

   @Message(value = "Remote cache name is missing or null in backup configuration.", id = 339)
   @Description("Cross-site replication backs up data to caches with the same name by default. If you want to backup to a cache with a different name, you must specify the name of the remote cache in the 'backup-for' configuration. Modify cache configuration to include the name of the remote cache.")
   CacheConfigurationException backupForNullCache();

   @Message(value = "Remote cache name and remote site is missing or null in backup configuration.", id = 340)
   @Description("Cross-site replication backs up data to caches with the same name by default. If you want to backup to a cache with a different name, you must specify the name of the remote cache and the remote site in the 'backup-for' configuration. Modify cache configuration to include the name of the remote cache and remote site.")
   CacheConfigurationException backupForMissingParameters();

//   @Message(value = "Cannot configure async properties for an sync cache. Set the cache mode to async first.", id = 341)
//   IllegalStateException asyncPropertiesConfigOnSyncCache();

//   @Message(value = "Cannot configure sync properties for an async cache. Set the cache mode to sync first.", id = 342)
//   IllegalStateException syncPropertiesConfigOnAsyncCache();

   @Message(value = "Must have a transport set in the global configuration in " +
         "order to define a clustered cache", id = 343)
   CacheConfigurationException missingTransportConfiguration();

   @Message(value = "reaperWakeUpInterval must be >= 0, we got %d", id = 344)
   CacheConfigurationException invalidReaperWakeUpInterval(long timeout);

   @Message(value = "completedTxTimeout must be >= 0, we got %d", id = 345)
   CacheConfigurationException invalidCompletedTxTimeout(long timeout);

//   @Message(value = "Total Order based protocol not available for transaction mode %s", id = 346)
//   CacheConfigurationException invalidTxModeForTotalOrder(TransactionMode transactionMode);

//   @Message(value = "Cache mode %s is not supported by Total Order based protocol", id = 347)
//   CacheConfigurationException invalidCacheModeForTotalOrder(String friendlyCacheModeString);

//   @Message(value = "Total Order based protocol not available with recovery", id = 348)
//   CacheConfigurationException unavailableTotalOrderWithTxRecovery();

//   @Message(value = "Total Order based protocol not available with %s", id = 349)
//   CacheConfigurationException invalidLockingModeForTotalOrder(LockingMode lockingMode);

   @Message(value = "Enabling the L1 cache is only supported when using DISTRIBUTED as a cache mode.  Your cache mode is set to %s", id = 350)
   CacheConfigurationException l1OnlyForDistributedCache(String cacheMode);

   @Message(value = "Using a L1 lifespan of 0 or a negative value is meaningless", id = 351)
   CacheConfigurationException l1InvalidLifespan();

   @Message(value = "Enabling the L1 cache is not supported when using EXCEPTION based eviction.", id = 352)
   CacheConfigurationException l1NotValidWithExpirationEviction();

   @Message(value = "Cannot define both interceptor class (%s) and interceptor instance (%s)", id = 354)
   CacheConfigurationException interceptorClassAndInstanceDefined(String customInterceptorClassName, String customInterceptor);

   @Message(value = "Unable to instantiate loader/writer instance for StoreConfiguration %s", id = 355)
   CacheConfigurationException unableToInstantiateClass(Class<?> storeConfigurationClass);

//   @Message(value = "Maximum data container size is currently 2^48 - 1, the number provided was %s", id = 356)
//   CacheConfigurationException evictionSizeTooLarge(long value);

//   @LogMessage(level = ERROR)
//   @Message(value = "end() failed for %s", id = 357)
//   void xaResourceEndFailed(XAResource resource, @Cause Throwable t);

   @Message(value = "A cache configuration named %s already exists. This cannot be configured externally by the user.", id = 358)
   CacheConfigurationException existingConfigForInternalCache(String name);

   @Message(value = "Keys '%s' are not available. Not all owners are in this partition", id = 359)
   AvailabilityException degradedModeKeysUnavailable(Collection<?> keys);

   @LogMessage(level = WARN)
   @Message(value = "The xml element eviction-executor has been deprecated and replaced by expiration-executor, please update your configuration file.", id = 360)
   void evictionExecutorDeprecated();

   @Message(value = "Cannot commit remote transaction %s as it was already rolled back", id = 361)
   CacheException remoteTransactionAlreadyRolledBack(GlobalTransaction gtx);

   @Message(value = "Could not find status for remote transaction %s, please increase transaction.completedTxTimeout", id = 362)
   TimeoutException remoteTransactionStatusMissing(GlobalTransaction gtx);

   @LogMessage(level = WARN)
   @Message(value = "No filter indexing service provider found for indexed filter of type %s", id = 363)
   void noFilterIndexingServiceProviderFound(String filterClassName);

   @Message(value = "Attempted to register cluster listener of class %s, but listener is annotated as only observing pre events!", id = 364)
   CacheException clusterListenerRegisteredWithOnlyPreEvents(Class<?> listenerClass);

   @Message(value = "Could not find the specified JGroups configuration file '%s'", id = 365)
   CacheConfigurationException jgroupsConfigurationNotFound(String cfg);

//   @Message(value = "Unable to add a 'null' Custom Cache Store", id = 366)
//   IllegalArgumentException unableToAddNullCustomStore();

//   @LogMessage(level = ERROR)
//   @Message(value = "There was an issue with topology update for topology: %s", id = 367)
//   void topologyUpdateError(int topologyId, @Cause Throwable t);

//   @LogMessage(level = WARN)
//   @Message(value = "Memory approximation calculation for eviction is unsupported for the '%s' Java VM", id = 368)
//   void memoryApproximationUnsupportedVM(String javaVM);

//   @LogMessage(level = WARN)
//   @Message(value = "Ignoring asyncMarshalling configuration", id = 369)
//   void ignoreAsyncMarshalling();

//   @Message(value = "Cache name '%s' cannot be used as it is a reserved, internal name", id = 370)
//   IllegalArgumentException illegalCacheName(String name);

   @Message(value = "Cannot remove cache configuration '%s' because it is in use", id = 371)
   IllegalStateException configurationInUse(String configurationName);

//   @Message(value = "Statistics are enabled while attribute 'available' is set to false.", id = 372)
//   CacheConfigurationException statisticsEnabledNotAvailable();

   @Message(value = "Attempted to start a cache using configuration template '%s'", id = 373)
   CacheConfigurationException templateConfigurationStartAttempt(String cacheName);

   @Message(value = "No such template '%s' when declaring '%s'", id = 374)
   CacheConfigurationException undeclaredConfiguration(String template, String name);

   @Message(value = "No such template/configuration '%s'", id = 375)
   CacheConfigurationException noConfiguration(String extend);

   @Message(value = "Interceptor stack is not supported in simple cache", id = 376)
   UnsupportedOperationException interceptorStackNotSupported();

   @Message(value = "Explicit lock operations are not supported in simple cache", id = 377)
   UnsupportedOperationException lockOperationsNotSupported();

   @Message(value = "Invocation batching not enabled in current configuration! Please enable it.", id = 378)
   CacheConfigurationException invocationBatchingNotEnabled();

//   @Message(value = "Distributed Executors Framework is not supported in simple cache", id = 380)
//   CacheConfigurationException distributedExecutorsNotSupported();

   @Message(value = "This configuration is not supported for simple cache", id = 381)
   CacheConfigurationException notSupportedInSimpleCache();

   @LogMessage(level = WARN)
   @Message(value = "Global state persistence was enabled without specifying a location", id = 382)
   void missingGlobalStatePersistentLocation();

//   @LogMessage(level = WARN)
//   @Message(value = "The eviction max-entries attribute has been deprecated. Please use the size attribute instead", id = 383)
//   void evictionMaxEntriesDeprecated();

   @Message(value = "Unable to broadcast invalidation messages", id = 384)
   RuntimeException unableToBroadcastInvalidation(@Cause Throwable e);

//   @LogMessage(level = WARN)
//   @Message(value = "The data container class configuration has been deprecated.  This has no current replacement", id = 385)
//   void dataContainerConfigurationDeprecated();

   @Message(value = "Failed to read persisted state from file %s. Aborting.", id = 386)
   CacheConfigurationException failedReadingPersistentState(@Cause IOException e, File stateFile);

   @Message(value = "Failed to write state to file %s.", id = 387)
   CacheConfigurationException failedWritingGlobalState(@Cause IOException e, File stateFile);

   @Message(value = "The state file %s is not writable. Aborting.", id = 388)
   CacheConfigurationException nonWritableStateFile(File stateFile);

   @LogMessage(level = INFO)
   @Message(value = "Loaded global state, version=%s timestamp=%s", id = 389)
   void globalStateLoad(String version, String timestamp);

   @LogMessage(level = INFO)
   @Message(value = "Persisted state, version=%s timestamp=%s", id = 390)
   void globalStateWrite(String version, String timestamp);

   @Message(value = "Recovery not supported with non transactional cache", id = 391)
   CacheConfigurationException recoveryNotSupportedWithNonTxCache();

   @Message(value = "Recovery not supported with Synchronization", id = 392)
   CacheConfigurationException recoveryNotSupportedWithSynchronization();

   //@Message(value = "Recovery not supported with Asynchronous %s cache mode", id = 393)
   //CacheConfigurationException recoveryNotSupportedWithAsync(String cacheMode);

   //@Message(value = "Recovery not supported with asynchronous commit phase", id = 394)
   //CacheConfigurationException recoveryNotSupportedWithAsyncCommit();

   @LogMessage(level = INFO)
   @Message(value = "Transaction notifications are disabled.  This prevents cluster listeners from working properly!", id = 395)
   void transactionNotificationsDisabled();

   @LogMessage(level = DEBUG)
   @Message(value = "Received unsolicited state from node %s for segment %d of cache %s", id = 396)
   void ignoringUnsolicitedState(Address node, int segment, String cacheName);

//   @Message(value = "Could not migrate data for cache %s, check remote store config in the target cluster. Make sure only one remote store is present and is pointing to the source cluster", id = 397)
//   CacheException couldNotMigrateData(String name);

   @Message(value = "CH Factory '%s' cannot restore a persisted CH of class '%s'", id = 398)
   IllegalStateException persistentConsistentHashMismatch(String hashFactory, String consistentHashClass);

   @Message(value = "Timeout while waiting for %d members in cluster. Last view had %s", id = 399)
   TimeoutException timeoutWaitingForInitialNodes(int initialClusterSize, List<?> members);

   @Message(value = "Node %s was suspected", id = 400)
   @Description("A node in the cluster is offline or cannot be reached on the network. If you are using cross-site replication this message indicates that the relay nodes are not reachable. Check network settings for all nodes in the cluster.")
   SuspectException remoteNodeSuspected(Address address);

   @Message(value = "Node %s timed out, time : %s %s", id = 401)
   TimeoutException remoteNodeTimedOut(Address address, long time, TimeUnit unit);

   @Message(value = "Timeout waiting for view %d. Current view is %d, current status is %s", id = 402)
   TimeoutException coordinatorTimeoutWaitingForView(int expectedViewId, int currentViewId,
                                                     Object clusterManagerStatus);

   @Message(value = "No indexable classes were defined for this indexed cache. The configuration must contain " +
         "classes or protobuf message types annotated with '@Indexed'", id = 403)
   CacheConfigurationException noIndexableClassesDefined();

   @Message(value = "The configured entity class %s is not indexable. Please remove it from the indexing configuration.", id = 404)
   CacheConfigurationException classNotIndexable(String className);

   @LogMessage(level = ERROR)
   @Message(value = "Caught exception while invoking a cache manager listener!", id = 405)
   void failedInvokingCacheManagerListener(@Cause Throwable e);

   @LogMessage(level = WARN)
   @Message(value = "The replication queue is no longer supported since version 9.0. Attribute %s on line %d will be ignored.", id = 406)
   void ignoredReplicationQueueAttribute(String attributeName, int line);

   @Message(value = "Extraneous members %s are attempting to join cache %s, as they were not members of the persisted state", id = 407)
   CacheJoinException extraneousMembersJoinRestoredCache(List<Address> extraneousMembers, String cacheName);

   @Message(value = "Node %s with persistent state attempting to join cache %s on cluster without state", id = 408)
   CacheJoinException nodeWithPersistentStateJoiningClusterWithoutState(Address joiner, String cacheName);

   @Message(value = "Node %s without persistent state attempting to join cache %s on cluster with state", id = 409)
   CacheJoinException nodeWithoutPersistentStateJoiningCacheWithState(Address joiner, String cacheName);

   @Message(value = "Node %s attempting to join cache %s with incompatible state", id = 410)
   CacheJoinException nodeWithIncompatibleStateJoiningCache(Address joiner, String cacheName);

//   @LogMessage(level = WARN)
//   @Message(value = "Classpath does not look correct. Make sure you are not mixing uber and jars", id = 411)
//   void warnAboutUberJarDuplicates();

   @Message(value = "Cannot determine a synthetic transaction configuration from mode=%s, xaEnabled=%s, recoveryEnabled=%s, batchingEnabled=%s", id = 412)
   CacheConfigurationException unknownTransactionConfiguration(org.infinispan.transaction.TransactionMode mode, boolean xaEnabled, boolean recoveryEnabled, boolean batchingEnabled);

   @Message(value = "Unable to instantiate serializer for %s", id = 413)
   CacheConfigurationException unableToInstantiateSerializer(Class<?> storeConfigurationClass);

   @Message(value = "Global security authorization should be enabled if cache authorization enabled.", id = 414)
   CacheConfigurationException globalSecurityAuthShouldBeEnabled();

   @LogMessage(level = WARN)
   @Message(value = "The %s is no longer supported since version %s. Attribute %s on line %d will be ignored.", id = 415)
   void ignoredAttribute(String componentName, String version, String attributeName, int line);

//   @LogMessage(level = ERROR)
//   @Message(value = "Error executing submitted store task", id = 416)
//   void errorExecutingSubmittedStoreTask(@Cause Throwable cause);

   @Message(value = "It is not possible for a store to be transactional in a non-transactional cache. ", id = 417)
   CacheConfigurationException transactionalStoreInNonTransactionalCache();

   @Message(value = "It is not possible for a store to be transactional when passivation is enabled. ", id = 418)
   CacheConfigurationException transactionalStoreInPassivatedCache();

   @LogMessage(level = WARN)
   @Message(value = "Eviction of an entry invoked without an explicit eviction strategy for cache %s", id = 419)
   void evictionDisabled(String cacheName);

   @Message(value = "Cannot enable '%s' in invalidation caches!", id = 420)
   CacheConfigurationException attributeNotAllowedInInvalidationMode(String attributeName);

   @LogMessage(level = ERROR)
   @Message(value = "Error while handling view %s", id = 421)
   void viewHandlingError(int viewId, @Cause Throwable t);

   @Message(value = "Failed waiting for topology %d", id = 422)
   TimeoutException failedWaitingForTopology(int requestTopologyId);

   @Message(value = "Duplicate id found! AdvancedExternalizer id=%d is shared by another externalizer (%s)", id = 423)
   CacheConfigurationException duplicateExternalizerIdFound(int externalizerId, String otherExternalizer);

   @Message(value = "Memory eviction is enabled, please specify a maximum size or count greater than zero", id = 424)
   CacheConfigurationException invalidEvictionSize();

//   @Message(value = "Eviction cannot use memory-based approximation with LIRS", id = 425)
//   CacheConfigurationException memoryEvictionInvalidStrategyLIRS();

   //removed unused message (id=426)

   @Message(value = "Timeout after %s waiting for acks (%s). Id=%s, Topology Id=%s", id = 427)
   TimeoutException timeoutWaitingForAcks(String timeout, String address, long id, int topology);

   @LogMessage(level = WARN)
   @Message(value = "'%1$s' at %3$s has been deprecated. Please use '%2$s' instead", id = 428)
   void configDeprecatedUseOther(Enum<?> element, Enum<?> other, Location location);

   @Message(value = "On key %s previous read version (%s) is different from currently read version (%s)", id = 429)
   WriteSkewException writeSkewOnRead(@Param Object key, Object key2, EntryVersion lastVersion, EntryVersion remoteVersion);

   @Message(value = "%s cannot be shared", id = 430)
   CacheConfigurationException nonSharedStoreConfiguredAsShared(String storeType);

//   @LogMessage(level = WARN)
//   @Message(value = "Unable to validate %s's configuration as the @Store annotation is missing", id = 431)
//   void warnStoreAnnotationMissing(String name);

   @Message(value = "Missing configuration for default cache '%s' declared on container", id = 432)
   CacheConfigurationException missingDefaultCacheDeclaration(String defaultCache);

   @Message(value = "A default cache has been requested, but no cache has been set as default for this container", id = 433)
   CacheConfigurationException noDefaultCache();

//   @LogMessage(level = WARN)
//   @Message(value = "Direct usage of the ___defaultcache name to retrieve the default cache is deprecated", id = 434)
//   void deprecatedDefaultCache();

   @Message(value = "Cache manager initialized with a default cache configuration but without a name for it. Set it in the GlobalConfiguration.", id = 435)
   CacheConfigurationException defaultCacheConfigurationWithoutName();

   @Message(value = "Cache '%s' has been requested, but no matching cache configuration exists", id = 436)
   CacheConfigurationException noSuchCacheConfiguration(String name);

   @LogMessage(level = WARN)
   @Message(value = "Unable to validate '%s' with the implementing store as the @ConfigurationFor annotation is missing", id = 437)
   void warnConfigurationForAnnotationMissing(String name);

   @Message(value = "Cache with name '%s' is defined more than once!", id = 438)
   CacheConfigurationException duplicateCacheName(String name);

   @LogMessage(level = INFO)
   @Message(value = "Received new cross-site view: %s", id = 439)
   @Description("A cluster has either joined or left the global cluster view.")
   void receivedXSiteClusterView(Collection<String> view);

   @LogMessage(level = ERROR)
   @Message(value = "Error sending response for request %d@%s, command %s", id = 440)
   void errorSendingResponse(long requestId, org.jgroups.Address origin, Object command);

   @Message(value = "Unsupported async cache mode '%s' for transactional caches", id = 441)
   CacheConfigurationException unsupportedAsyncCacheMode(CacheMode cacheMode);

//   @Message(value = "Invalid cache loader configuration for '%s'.  If a cache loader is configured as a singleton  , the cache loader cannot be shared in a cluster!", id = 442)
//   CacheConfigurationException singletonStoreCannotBeShared(String name);

   @Message(value = "Invalid cache loader configuration for '%s'. In order for a cache loader to be transactional, it must also be shared.", id = 443)
   CacheConfigurationException clusteredTransactionalStoreMustBeShared(String simpleName);

   @Message(value = "Invalid cache loader configuration for '%s'. A cache loader cannot be both Asynchronous and transactional.", id = 444)
   CacheConfigurationException transactionalStoreCannotBeAsync(String simpleName);

//   @Message(value = "At most one store can be set to 'fetchPersistentState'!", id = 445)
//   CacheConfigurationException onlyOneFetchPersistentStoreAllowed();

   @Message(value = "Multiple sites have the same name '%s'. This configuration is not valid.", id = 446)
   @Description("The name for each cluster that participates in cross-site replication must have a unique site name. Modify JGroups RELAY2 configuration and specify a unique site name for each backup location.")
   CacheConfigurationException multipleSitesWithSameName(String site);

//   @Message(value = "The site '%s' must be defined within the set of backups!", id = 447)
//   CacheConfigurationException siteMustBeInBackups(String site);

   @Message(value = "'awaitInitialTransfer' can be enabled only if cache mode is distributed or replicated.", id = 448)
   CacheConfigurationException awaitInitialTransferOnlyForDistOrRepl();

   @Message(value = "Timeout value for cross-site replication state transfer must be equal to or greater than one.", id = 449)
   @Description("The value of the timeout attribute is zero or a negative number. Specify a value of at least one for the timeout attribute in the cross-site state transfer configuration for your cache.")
   CacheConfigurationException invalidXSiteStateTransferTimeout();

   @Message(value = "Wait time between retries for cross-site replication state transfer must be equal to or greater than one.", id = 450)
   @Description("The value of the wait-time attribute is zero or a negative number. Specify a value of at least one for the wait-time attribute in the cross-site state transfer configuration for your cache.")
   CacheConfigurationException invalidXSiteStateTransferWaitTime();

   @Message(value = "Timed out waiting for view %d, current view is %d", id = 451)
   TimeoutException timeoutWaitingForView(int expectedViewId, int currentViewId);

   @LogMessage(level = ERROR)
   @Message(value = "Failed to update topology for cache %s", id = 452)
   void topologyUpdateError(String cacheName, @Cause Throwable cause);

   @Message(value = "Attempt to define configuration for cache %s which already exists", id = 453)
   CacheConfigurationException configAlreadyDefined(String cacheName);

   @LogMessage(level = ERROR)
   @Message(value = "Failure during leaver transactions cleanup", id = 455)
   void transactionCleanupError(@Cause Throwable e);

//   @Message(value = "Cache does not contain the atomic map.", id = 456)
//   IllegalStateException atomicMapDoesNotExist();

//   @Message(value = "Cache contains %s which is not of expected type %s", id = 457)
//   IllegalStateException atomicMapHasWrongType(Object value, Class<?> type);

//   @Message(value = "Fine grained maps require clustering.hash.groups enabled.", id = 458)
//   IllegalStateException atomicFineGrainedNeedsGroups();

//   @Message(value = "Fine grained maps require transactional cache.", id = 459)
//   IllegalStateException atomicFineGrainedNeedsTransactions();

//   @Message(value = "Fine grained maps require explict transaction or auto-commit enabled", id = 460)
//   IllegalStateException atomicFineGrainedNeedsExplicitTxOrAutoCommit();

   @Message(value = "Class %s should be a subclass of %s", id = 461)
   CacheException invalidEncodingClass(Class<?> configured, Class<?> required);

   @Message(value = "ConflictManager.getConflicts() already in progress", id = 462)
   IllegalStateException getConflictsAlreadyInProgress();

   @Message(value = "Unable to retrieve conflicts as StateTransfer is currently in progress for cache '%s'", id = 463)
   IllegalStateException getConflictsStateTransferInProgress(String cacheName);

   @LogMessage(level = WARN)
   @Message(value = "The partition handling 'enabled' attribute has been deprecated. Please update your configuration to use 'when-split' instead", id = 464)
   void partitionHandlingConfigurationEnabledDeprecated();

//   @Message(value = "Keys '%s' are not available. No owners exist in this partition", id = 465)
//   AvailabilityException degradedModeNoOwnersExist(Object key);

   @LogMessage(level = WARN)
   @Message(value = "Exception encountered when trying to resolve conflict on Keys '%s': %s", id = 466)
   void exceptionDuringConflictResolution(Object key, Throwable t);

   @Message(value = "Cache manager is stopping", id = 472)
   IllegalLifecycleStateException cacheManagerIsStopping();

   @LogMessage(level = ERROR)
   @Message(value = "Invalid message type %s received from %s", id = 473)
   void invalidMessageType(int messageType, org.jgroups.Address origin);

   @LogMessage(level = ERROR)
   @Message(value = "Error processing request %d@%s", id = 474)
   void errorProcessingRequest(long requestId, org.jgroups.Address origin, @Cause Throwable t);

   @LogMessage(level = ERROR)
   @Message(value = "Error processing response for request %d from %s", id = 475)
   void errorProcessingResponse(long requestId, org.jgroups.Address sender, @Cause Throwable t);

   @Message(value = "Timed out waiting for responses for request %d from %s after %s", id = 476)
   TimeoutException requestTimedOut(long requestId, String targetsWithoutResponses, String elapsed);

   @LogMessage(level = ERROR)
   @Message(value = "Cannot perform operation %s for site %s", id = 477)
   @Description("It was not possible to successfully complete an operation on a backup location. Set logging levels to TRACE to analyze and troubleshoot the issue.")
   void xsiteAdminOperationError(String operationName, String siteName, @Cause Throwable t);

   @Message(value = "Couldn't find a local transaction corresponding to remote site transaction %s", id = 478)
   CacheException unableToFindRemoteSiteTransaction(GlobalTransaction globalTransaction);

   @Message(value = "LocalTransaction not found but present in the tx table for remote site transaction %s", id = 479)
   IllegalStateException unableToFindLocalTransactionFromRemoteSiteTransaction(GlobalTransaction globalTransaction);

//   @LogMessage(level = WARN)
//   @Message(value = "Ignoring versions invalidation from topology %d, current topology is %d", id = 480)
//   void ignoringInvalidateVersionsFromOldTopology(int invalidationTopology, int currentTopologyId);

   @Message(value = "Cannot create remote transaction %s, the originator is not in the cluster view", id = 481)
   CacheException remoteTransactionOriginatorNotInView(GlobalTransaction gtx);

   @Message(value = "Cannot create remote transaction %s, already completed", id = 482)
   CacheException remoteTransactionAlreadyCompleted(GlobalTransaction gtx);

//   @Message(value = "Class %s not found", id = 483)
//   CacheConfigurationException classNotFound(String name);

   @Message(value = "Wildcards not allowed in cache names: '%s'", id = 484)
   CacheConfigurationException wildcardsNotAllowedInCacheNames(String name);

   @Message(value = "Configuration '%s' matches multiple wildcard templates", id = 485)
   CacheConfigurationException configurationNameMatchesMultipleWildcards(String name);

   @Message(value = "Cannot register Wrapper: duplicate Id %d", id = 486)
   EncodingException duplicateIdWrapper(byte id);

   @Message(value = "Wrapper with class '%s' not found", id = 487)
   EncodingException wrapperClassNotFound(Class<?> wrapperClass);

   @Message(value = "Wrapper with Id %d not found", id = 488)
   EncodingException wrapperIdNotFound(byte id);

   @Message(value = "Cannot register Encoder: duplicate Id %d", id = 489)
   EncodingException duplicateIdEncoder(short id);

   @Message(value = "Encoder with class '%s' not found", id = 490)
   EncodingException encoderClassNotFound(Class<?> wrapperClass);

   @Message(value = "Encoder with Id %d not found", id = 491)
   EncodingException encoderIdNotFound(short id);

   @Message(value = "Cannot find transcoder between '%s' to '%s'", id = 492)
   EncodingException cannotFindTranscoder(MediaType mediaType, MediaType another);

//   @Message(value = "Invalid text format: '%s'", id = 493)
//   EncodingException invalidTextFormat(Object content);

//   @Message(value = "Invalid binary format: '%s'", id = 494)
//   EncodingException invalidBinaryFormat(Object content);

   @Message(value = "%s encountered error transcoding content", id = 495)
   EncodingException errorTranscoding(String transcoderName, @Cause Throwable cause);

//   @Message(value = "Error transcoding content '%s'", id = 496)
//   EncodingException errorTranscodingContent(@Cause Throwable cause, Object content);

   @Message(value = "%s encountered unsupported content '%s' during transcoding", id = 497)
   EncodingException unsupportedContent(String transcoderName, Object content);

//   @LogMessage(level = WARN)
//   @Message(value = "Indexing mode ALL without owning all data locally (replicated mode).", id = 498)
//   void allIndexingInNonReplicatedCache();

   @Message(value = "Could not serialize the configuration of cache '%s' (%s)", id = 499)
   CacheConfigurationException configurationSerializationFailed(String cacheName, Configuration configuration, @Cause Exception e);

   @Message(value = "Cannot create clustered configuration for cache '%s' because configuration %n%s%n is incompatible with the existing configuration %n%s", id = 500)
   CacheConfigurationException incompatibleClusterConfiguration(String cacheName, Configuration configuration, Configuration existing);

   @Message(value = "Cannot persist cache configuration as global state is disabled", id = 501)
   CacheConfigurationException globalStateDisabled();

   @Message(value = "Error while persisting global configuration state", id = 502)
   CacheConfigurationException errorPersistingGlobalConfiguration(@Cause Throwable cause);

   @Message(value = "Size (bytes) based eviction needs either off-heap or a binary compatible storage configured in the cache encoding", id = 504)
   CacheConfigurationException offHeapMemoryEvictionNotSupportedWithObject();

   @Message(value = "Cache %s already exists", id = 507)
   CacheConfigurationException cacheExists(String cacheName);

   @Message(value = "Cannot rename file %s to %s", id = 508)
   CacheConfigurationException cannotRenamePersistentFile(String absolutePath, File persistentFile, @Cause Throwable cause);

   @Message(value = "Unable to add a 'null' EntryMergePolicyFactory", id = 509)
   IllegalArgumentException unableToAddNullEntryMergePolicyFactory();

   @Message(value = "ConfigurationStrategy set to CUSTOM, but none specified", id = 510)
   CacheConfigurationException customStorageStrategyNotSet();

   @Message(value = "ConfigurationStrategy cannot be set to MANAGED in embedded mode", id = 511)
   CacheConfigurationException managerConfigurationStorageUnavailable();

   @Message(value = "Cannot acquire lock '%s' for persistent global state", id = 512)
   CacheConfigurationException globalStateCannotAcquireLockFile(@Cause Throwable cause, FileSystemLock lockFile);

   @Message(value = "Exception based eviction requires a transactional cache that doesn't allow for 1 phase commit or synchronizations", id = 513)
   CacheConfigurationException exceptionBasedEvictionOnlySupportedInTransactionalCaches();

   @Message(value = "Container eviction limit %d reached, write operation(s) is blocked", id = 514)
   ContainerFullException containerFull(long size);

   @Message(value = "The configuration is immutable", id = 515)
   UnsupportedOperationException immutableConfiguration();

   @Message(value = "The state file for '%s' is invalid. Startup halted to prevent further corruption of persistent state", id = 516)
   CacheConfigurationException invalidPersistentState(String globalScope);

   @LogMessage(level = WARN)
   @Message(value = "Ignoring cache topology from %s during merge: %s", id = 517)
   void ignoringCacheTopology(Collection<Address> sender, CacheTopology topology);

   @LogMessage(level = DEBUG)
   @Message(value = "Updating topology for cache %s, topology %s, availability mode %s", id = 518)
   void updatingTopology(String cacheName, CacheTopology currentTopology, AvailabilityMode availabilityMode);

   @LogMessage(level = DEBUG)
   @Message(value = "Updating stable topology for cache %s, topology %s", id = 519)
   void updatingStableTopology(String cacheName, CacheTopology currentTopology);

   @LogMessage(level = DEBUG)
   @Message(value = "Updating availability mode for cache %s from %s to %s, topology %s", id = 520)
   void updatingAvailabilityMode(String cacheName, AvailabilityMode oldMode, AvailabilityMode newMode, CacheTopology topology);

   @LogMessage(level = DEBUG)
   @Message(value = "Cache %s recovered after merge with topology = %s, availability mode %s", id = 521)
   void cacheRecoveredAfterMerge(String cacheName, CacheTopology currentTopology, AvailabilityMode availabilityMode);

   @LogMessage(level = DEBUG)
   @Message(value = "Conflict resolution starting for cache %s with topology %s", id = 522)
   void startingConflictResolution(String cacheName, CacheTopology currentTopology);

   @LogMessage(level = DEBUG)
   @Message(value = "Conflict resolution finished for cache %s with topology %s", id = 523)
   void finishedConflictResolution(String cacheName, CacheTopology currentTopology);

   @LogMessage(level = ERROR)
   @Message(value = "Conflict resolution failed for cache %s with topology %s", id = 524)
   void failedConflictResolution(String cacheName, CacheTopology currentTopology, @Cause Throwable t);

   @LogMessage(level = DEBUG)
   @Message(value = "Conflict resolution cancelled for cache %s with topology %s", id = 525)
   void cancelledConflictResolution(String cacheName, CacheTopology currentTopology);

   @Message(value = "Maximum startup attempts exceeded for store %s", id = 527)
   PersistenceException storeStartupAttemptsExceeded(String storeName, @Cause Throwable t);

   @Message(value = "Cannot acquire lock %s as this partition is DEGRADED", id = 528)
   AvailabilityException degradedModeLockUnavailable(Object key);

   @Message(value = "Class '%s' blocked by deserialization allow list. Include the class name in the server allow list to authorize.", id = 529)
   CacheException errorDeserializing(String className);

   @LogMessage(level = WARN)
   @Message(value = "Unsupported async cache mode '%s' for transactional caches, forcing %s", id = 530)
   void unsupportedAsyncCacheMode(CacheMode unsupportedCacheMode, CacheMode forcedCacheMode);

   @Message(value = "Store or loader %s must implement SegmentedLoadWriteStore or its config must extend AbstractSegmentedStoreConfiguration if configured as segmented", id = 531)
   CacheConfigurationException storeNotSegmented(Class<?> implementedClass);

   @Message(value = "Invalid cache loader configuration for '%s'.  If a cache loader is configured with passivation, the cache loader cannot be shared in a cluster!", id = 532)
   CacheConfigurationException passivationStoreCannotBeShared(String name);

   @Message(value = "Content '%s (MediaType: '%s') cannot be converted to '%s'", id = 533)
   EncodingException cannotConvertContent(Object content, MediaType contentType, MediaType destination, @Cause Throwable e);

   @Message(value = "Grouping requires OBJECT storage type but was: %s", id = 534)
   CacheConfigurationException groupingOnlyCompatibleWithObjectStorage(StorageType storageType);

   @Message(value = "Grouping requires application/x-java-object storage type but was: {key=%s, value=%s}", id = 535)
   CacheConfigurationException groupingOnlyCompatibleWithObjectStorage(MediaType keyMediaType, MediaType valueMediaType);

   @Message(value = "Factory doesn't know how to construct component %s", id = 537)
   CacheConfigurationException factoryCannotConstructComponent(String componentName);

   @LogMessage(level = ERROR)
   @Message(value = "Error stopping module %s", id = 538)
   void moduleStopError(String module, @Cause Throwable t);

   @Message(value = "Duplicate JGroups stack '%s'", id = 539)
   CacheConfigurationException duplicateJGroupsStack(String name);

   @Message(value = "No such JGroups stack '%s'", id = 540)
   CacheConfigurationException missingJGroupsStack(String name);

   @Message(value = "Error while trying to create a channel using the specified configuration '%s'", id = 541)
   CacheConfigurationException errorCreatingChannelFromConfigurator(String configurator, @Cause Throwable t);

   @Message(value = "Invalid parser scope. Expected '%s' but was '%s'", id = 542)
   CacheConfigurationException invalidScope(String expected, String found);

   @Message(value = "Cannot use stack.position when stack.combine is '%s'", id = 543)
   CacheConfigurationException jgroupsNoStackPosition(String combineMode);

   @Message(value = "The protocol '%s' does not exist in the base stack for operation '%s'", id = 544)
   CacheConfigurationException jgroupsNoSuchProtocol(String protocolName, String combineMode);

   @Message(value = "Inserting protocol '%s' in a JGroups stack requires the 'stack.position' attribute", id = 545)
   CacheConfigurationException jgroupsInsertRequiresPosition(String protocolName);

   @Message(value = "Duplicate remote site '%s' in stack '%s'", id = 546)
   @Description("The name for each cluster that participates in cross-site replication must have a unique site name. Modify JGroups RELAY2 configuration and specify a unique site name for each backup location.")
   CacheConfigurationException duplicateRemoteSite(String remoteSite, String name);

   @Message(value = "JGroups stack '%s' declares remote sites but does not include the RELAY2 protocol.", id = 547)
   @Description("Cross-site replication requires the JGroups RELAY2 protocol. Modify the JGroups configuration to include a RELAY2 stack.")
   CacheConfigurationException jgroupsRemoteSitesWithoutRelay(String name);

   @Message(value = "JGroups stack '%s' has a RELAY2 protocol without remote sites.", id = 548)
   @Description("Each cluster that participates in cross-site replication must be identified with a site name in the RELAY2 stack. Modify JGroups configuration and specify a unique site name for each backup location.")
   CacheConfigurationException jgroupsRelayWithoutRemoteSites(String name);

   @Message(value = "A store cannot be shared when utilised with a local cache.", id = 549)
   CacheConfigurationException sharedStoreWithLocalCache();

   @Message(value = "Invalidation mode only supports when-split=ALLOW_READ_WRITES", id = 550)
   CacheConfigurationException invalidationPartitionHandlingNotSuported();

   @LogMessage(level = WARN)
   @Message(value = "The custom interceptors configuration has been deprecated and are ignored", id = 551)
   void customInterceptorsIgnored();

//   @LogMessage(level = WARN)
//   @Message(value = "Module '%s' provides an instance of the deprecated ModuleCommandInitializer. Commands that require initialization should implement the InitializableCommand interface", id = 552)
//   void warnModuleCommandInitializerDeprecated(String module);

   @LogMessage(level = WARN)
   @Message(value = "Ignoring 'marshaller' attribute. Common marshallers are already available at runtime, and to deploy a custom marshaller, consult the 'Encoding' section in the user guide", id = 553)
   void marshallersNotSupported();

//   @LogMessage(level = WARN)
//   @Message(value = "jboss-marshalling is deprecated and planned for removal", id = 554)
//   void jbossMarshallingDetected();

//   @LogMessage(level = ERROR)
//   @Message(value = "Unable to set method %s accessible", id = 555)
//   void unableToSetAccessible(Method m, @Cause Exception e);

   @LogMessage(level = INFO)
   @Message(value = "Starting user marshaller '%s'", id = 556)
   void startingUserMarshaller(String marshallerClass);

   @Message(value = "Unable to configure JGroups Stack '%s'", id = 557)
   CacheConfigurationException unableToAddJGroupsStack(String name, @Cause Exception e);

   @Message(value = "The store location '%s' is not a child of the global persistent location '%s'", id = 558)
   CacheConfigurationException forbiddenStoreLocation(Path location, Path global);

   @LogMessage(level = WARN)
   @Message(value = "Cannot marshall '%s'", id = 559)
   void cannotMarshall(Class<?> aClass, @Cause Throwable t);

   @LogMessage(level = WARN)
   @Message(value = "The AdvancedExternalizer configuration has been deprecated and will be removed in the future", id = 560)
   void advancedExternalizerDeprecated();

   @Message(value = "Chunk size must be positive, got %d", id = 561)
   CacheConfigurationException invalidChunkSize(int chunkSize);

   @Message(value = "Invalid cache loader configuration for '%s'.  If a cache loader is configured with purgeOnStartup, the cache loader cannot be shared in a cluster!", id = 562)
   CacheConfigurationException sharedStoreShouldNotBePurged(String name);

   @Message(value = "Invalid cache loader configuration for '%s'.  This implementation does not support being segmented!", id = 563)
   CacheConfigurationException storeDoesNotSupportBeingSegmented(String name);

//   @LogMessage(level = WARN)
//   @Message(value = "Configured store '%s' is segmented and may use a large number of file descriptors", id = 564)
//   void segmentedStoreUsesManyFileDescriptors(String storeName);

//   @Message(value = "Index.%s is no longer supported. Please update your configuration!", id = 565)
//   CacheConfigurationException indexModeNotSupported(String indexMode);

   @Message(value = "Thread Pool Factory %s is blocking, but pool %s requires non blocking threads", id = 566)
   CacheConfigurationException threadPoolFactoryIsBlocking(String name, String poolName);

//   @LogMessage(level = WARN)
//   @Message(value = "SerializationConfiguration Version is deprecated since version 10.1 and will be removed in the future. The configured value has no affect on Infinispan marshalling.", id = 567)
//   void serializationVersionDeprecated();

//   @Message(value = "Failed to initialize base and vendor metrics from JMX MBeans", id = 568)
//   IllegalStateException failedToInitBaseAndVendorMetrics(@Cause Exception e);

   @LogMessage(level = WARN)
   @Message(value = "Unable to persist Infinispan internal caches as no global state enabled", id = 569)
   @Once
   void warnUnableToPersistInternalCaches();

   @Message(value = "Unexpected response from %s: %s", id = 570)
   IllegalArgumentException unexpectedResponse(Address target, Response response);

   @Message(value = "RELAY2 not found in the protocol stack. Cannot perform cross-site operations.", id = 571)
   @Description("To back up caches from one site to another the cluster transport uses the JGroups RELAY2 protocol. Add RELAY2 to your cluster transport configuration.")
   CacheConfigurationException crossSiteUnavailable();

   @LogMessage(level = WARN)
   @Message(value = "index mode attribute is deprecated and should no longer be specified because its value is automatically detected. Most previously supported values are no longer supported. Please check the upgrade guide.", id = 572)
   void indexModeDeprecated();

   @Message(value = "Cannot recreate persisted configuration for cache '%s' because configuration %n%s%n is incompatible with the existing configuration %n%s", id = 573)
   CacheConfigurationException incompatiblePersistedConfiguration(String cacheName, Configuration configuration, Configuration existing);

   @LogMessage(level = WARN)
   @Message(value = "Global state cannot persisted because it is incomplete (usually caused by errors at startup).", id = 574)
   void incompleteGlobalState();

   @Message(value = "PartitionStrategy must be ALLOW_READ_WRITES when numOwners is 1", id = 575)
   CacheConfigurationException singleOwnerNotSetToAllowReadWrites();

   @Message(value = "Cross-site replication not available for local cache.", id = 576)
   @Description("Cross-site replication capabilities do not apply to local cache mode. Either remove the backup configuration from the local cache or use a distributed or replicated cache mode.")
   CacheConfigurationException xsiteInLocalCache();

   @Message(value = "Converting from unwrapped protostream payload requires the 'type' parameter to be supplied in the destination MediaType", id = 577)
   MarshallingException missingTypeForUnwrappedPayload();

   @LogMessage(level = INFO)
   @Message(value = "Migrating '%s' persisted data to new format...", id = 578)
   void startMigratingPersistenceData(String cacheName);

   @LogMessage(level = INFO)
   @Message(value = "'%s' persisted data successfully migrated.", id = 579)
   void persistedDataSuccessfulMigrated(String cacheName);

   @Message(value = "Failed to migrate '%s' persisted data.", id = 580)
   PersistenceException persistedDataMigrationFailed(String cacheName, @Cause Throwable cause);

//   @Message(value = "The indexing 'enabled' and the legacy 'index' configs attributes are mutually exclusive", id = 581)
//   CacheConfigurationException indexEnabledAndIndexModeAreExclusive();

//   @Message(value = "A single indexing directory provider is allowed per cache configuration. Setting multiple individual providers for the indexes belonging to a cache is not allowed.", id = 582)
//   CacheConfigurationException foundMultipleDirectoryProviders();

   @Message(value = "Cannot configure both maxCount and maxSize in memory configuration", id = 583)
   CacheConfigurationException cannotProvideBothSizeAndCount();

   @Message(value = "The memory attribute(s) %s have been deprecated and cannot be used in conjunction with the new configuration", id = 584)
   CacheConfigurationException cannotUseDeprecatedAndReplacement(String legacyName);

   @LogMessage(level = WARN)
   @Message(value = "Single media-type was specified for keys and values, ignoring individual configurations", id = 585)
   void ignoringSpecificMediaTypes();

   @LogMessage(level = WARN)
   @Message(value = "The memory configuration element '%s' has been deprecated. Please update your configuration", id = 586)
   void warnUsingDeprecatedMemoryConfigs(String element);

   @Message(value = "Cannot change max-size since max-count is already defined", id = 587)
   CacheConfigurationException cannotChangeMaxSize();

   @Message(value = "Cannot change max-count since max-size is already defined", id = 588)
   CacheConfigurationException cannotChangeMaxCount();

   @Message(value = "A store cannot be configured with both preload and purgeOnStartup", id = 589)
   CacheConfigurationException preloadAndPurgeOnStartupConflict();

   @Message(value = "Store cannot be configured with both read and write only!", id = 590)
   CacheConfigurationException storeBothReadAndWriteOnly();

   @Message(value = "Store cannot be configured with purgeOnStartup, shared or passivation if it is read only!", id = 591)
   CacheConfigurationException storeReadOnlyExceptions();

   @Message(value = "Store cannot be configured with fetchPersistenceState or preload if it is write only!", id = 592)
   CacheConfigurationException storeWriteOnlyExceptions();

   @Message(value = "Store %s cannot be configured to be %s as the implementation specifies it is already %s!", id = 593)
   CacheConfigurationException storeConfiguredHasBothReadAndWriteOnly(String storeClassName, NonBlockingStore.Characteristic configured,
         NonBlockingStore.Characteristic implSpecifies);

   @Message(value = "At most one store can be set to 'preload'!", id = 594)
   CacheConfigurationException onlyOnePreloadStoreAllowed();

   @LogMessage(level = WARN)
   @Message(value = "ClusterLoader has been deprecated and will be removed in a future version with no direct replacement", id = 595)
   void warnUsingDeprecatedClusterLoader();

//   @LogMessage(level = WARN)
//   @Message(value = "Indexing auto-config attribute is deprecated. Please check the upgrade guide.", id = 596)
//   void autoConfigDeprecated();

   @Message(value = "Store %s cannot be configured to be transactional as it does not contain the TRANSACTIONAL characteristic", id = 597)
   CacheConfigurationException storeConfiguredTransactionalButCharacteristicNotPresent(String storeClassName);

   @Message(value = "Store must specify a location when global state is disabled", id = 598)
   CacheConfigurationException storeLocationRequired();

   @Message(value = "Store '%s' must specify the '%s' attribute when global state is disabled", id = 598)
   CacheConfigurationException storeLocationRequired(String storeType, String attributeName);

   @LogMessage(level = WARN)
   @Message(value = "Configuration for cache '%s' does not define the encoding for keys or values. " +
         "If you use operations that require data conversion or queries, you should configure the " +
         "cache with a specific MediaType for keys or values.", id = 599)
   void unknownEncoding(String cacheName);

   @Message(value = "Store %s cannot be configured to be shared as it does not contain the SHARED characteristic", id = 600)
   CacheConfigurationException storeConfiguredSharedButCharacteristicNotPresent(String storeClassName);

   @Message(value = "Store %s cannot be configured to be segmented as it does not contain the SEGMENTABLE characteristic", id = 601)
   CacheConfigurationException storeConfiguredSegmentedButCharacteristicNotPresent(String storeClassName);

   @LogMessage(level = WARN)
   @Message(value = "Conversions between JSON and Java Objects are deprecated and will be removed in a future version. " +
         "To read/write values as JSON, it is recommended to define a protobuf schema and store data in the cache using " +
         "'application/x-protostream' as MediaType", id = 602)
   void jsonObjectConversionDeprecated();

   @Message(value = "Cannot handle cross-site request from site '%s'. Cache '%s' not found.", id = 603)
   @Description("A remote cluster attempted to replicate data to a cache that does not exist on the local cluster. Either create the cache or modify the backup configuration for the cache on the remote site.")
   CacheConfigurationException xsiteCacheNotFound(String remoteSite, ByteString cacheName);

   @Message(value = "Cannot handle cross-site request from site '%s'. Cache '%s' is stopped.", id = 604)
   @Description("A remote cluster attempted to replicate data to a cache that is not available. Start, or restart, the cache.")
   CrossSiteIllegalLifecycleStateException xsiteCacheNotStarted(String origin, ByteString cacheName);

   @Message(value = "Cannot handle cross-site request from site '%s'. Cache '%s' is not clustered.", id = 605)
   @Description("A remote cluster attempted to replicate data to a local cache. Either recreate the cache with a distributed or replicated mode or remove the backup configuration.")
   CacheConfigurationException xsiteInLocalCache(String origin, ByteString cacheName);

   @LogMessage(level = ERROR)
   @Message(value = "Remote site '%s' has an invalid cache configuration. Taking the backup location offline.", id = 606)
   @Description("An attempt was made to replicate data to a cache that does not have a valid configuration. Check the cache at the remote site and recreate it with a valid distributed or replicated configuration.")
   void xsiteInvalidConfigurationRemoteSite(String siteName, @Cause CacheConfigurationException exception);

   @Message(value = "The XSiteEntryMergePolicy is missing. The cache configuration must include a merge policy.", id = 607)
   @Description("To resolve conflicting entries between backup locations cache configuration must include a merge policy. Recreate the cache and specify a merge policy from the org.infinispan.xsite.spi.XSiteMergePolicy enum or use a conflict resolution algorithm.")
   CacheConfigurationException missingXSiteEntryMergePolicy();

   @LogMessage(level = FATAL)
   @Message(value = "[IRAC] Unexpected error occurred.", id = 608)
   @Description("During conflict resolution for cross-site replication an unexpected error occurred. To ensure data consistency between backup locations you should initiate state transfer to synchronize data between clusters.")
   void unexpectedErrorFromIrac(@Cause Throwable t);

   @LogMessage(level = DEBUG)
   @Message(value = "Cannot obtain cache '%s' as it is in FAILED state. Please check the configuration", id = 609)
   void cannotObtainFailedCache(String name, @Cause Throwable t);

   @Message(value = "Cache configuration must not declare indexed entities if it is not indexed", id = 610)
   CacheConfigurationException indexableClassesDefined();

   @Message(value = "Invalid index storage", id = 611)
   CacheConfigurationException invalidIndexStorage();

//   @LogMessage(level = WARN)
//   @Message(value = "Indexing configuration using properties has been deprecated and will be removed in a future " +
//         "version, please consult the docs for the replacements. The following properties have been found: '%s'", id = 612)
//   @Once
//   void indexingPropertiesDeprecated(Properties properties);

   @LogMessage(level = WARN)
   @Message(value = "Indexing configuration using properties has been deprecated and will be removed in a future " +
         "version, please use the <index-writer> and <index-reader> elements to configure indexing behavior.", id = 613)
   @Once
   void deprecatedIndexProperties();

   @Message(value = "It is not allowed to have different indexing configuration for each indexed type in a cache.", id = 614)
   CacheConfigurationException foundDifferentIndexConfigPerType();

   @Message(value = "Unable to unmarshall '%s' as a marshaller is not present in the user or global SerializationContext", id = 615)
   MarshallingException marshallerMissingFromUserAndGlobalContext(String type);

   @Message(value = "Unsupported persisted data version: %s", id = 616)
   PersistenceException persistedDataMigrationUnsupportedVersion(String magic);

   @Message(value = "Site '%s' not found.", id = 617)
   @Description("A backup location that is configured as a site in the JGroups RELAY2 stack is not available. Check the JGroups configuration and cache configuration to ensure that the remote site is configured correctly. If the configuration is correct then check that the backup location is online.")
   IllegalArgumentException siteNotFound(String siteName);

   @LogMessage(level = WARN)
   @Message(value = "Cleanup failed for cross-site state transfer. Invoke the cancel-push-state(%s) command if any nodes indicate pending operations to push state.", id = 618)
   @Description("When cross-site state transfer operations complete or fail due to a network timeout or other exception, the coordinator node sends a cancel-push-state command to other nodes. If any nodes indicate that there are pending operations to push state to a remote site, you can invoke the cancel-push-state command again on those nodes.")
   void xsiteCancelSendFailed(@Cause Throwable throwable, String remoteSite);

   @LogMessage(level = WARN)
   @Message(value = "Cleanup failed for cross-site state transfer. Invoke the cancel-receive(%s) command in site %s if any nodes indicate pending operations to receive state.", id = 619)
   @Description("When cross-site state transfer operations complete or fail due to a network timeout or other exception, the coordinator node sends a cancel-receive command to nodes. If any nodes indicate that there are pending operations to receive state from a remote site, you can invoke the cancel-receive command again.")
   void xsiteCancelReceiveFailed(@Cause Throwable throwable, String localSite, String remoteSite);

   @Message(value = "Cross-site state transfer to '%s' already started", id = 620)
   @Description("An attempt was made to initiate cross-site state transfer while the operation was already in progress. Wait for the state transfer operation to complete before initiating a subsequent operation. Alternatively you can cancel the cross-site state transfer operation that is in progress.")
   CacheException xsiteStateTransferAlreadyInProgress(String site);

   @Message(value = "Element '%1$s' has been removed at %3$s. Please use element '%2$s' instead", id = 621)
   CacheConfigurationException elementRemovedUseOther(String elementName, String newElementName, Location location);

   @Message(value = "Element '%s' at %s has been removed with no replacement", id = 622)
   CacheConfigurationException elementRemoved(String elementName, Location location);

   @Message(value = "Attribute '%1$s' has been removed at %3$s. Please use attribute '%2$s' instead", id = 623)
   CacheConfigurationException attributeRemovedUseOther(String attributeName, String newAttributeName, Location location);

   @Message(value = "Attribute '%2$s' of element '%1$s' at '%3$s' has been removed with no replacement", id = 624)
   CacheConfigurationException attributeRemoved(String elementName, String attributeName, Location location);

   @LogMessage(level = WARN)
   @Message(value = "Index path not provided and global state disabled, will use the current working directory for storage.", id = 625)
   void indexLocationWorkingDir();

   @LogMessage(level = WARN)
   @Message(value = "Index path '%s' is not absolute and global state is disabled, will use a dir relative to the current working directory.", id = 626)
   void indexRelativeWorkingDir(String path);

   @Message(value = "Invalid cache loader configuration for '%s'.  This implementation only supports being segmented!", id = 627)
   CacheConfigurationException storeRequiresBeingSegmented(String name);

   @Message(value = "Invalid cache roles '%s'", id = 628)
   CacheConfigurationException noSuchGlobalRoles(Set<String> cacheRoles);

   @LogMessage(level = WARN)
   @Message(value = "Exception completing partial completed transaction %s. Retrying later.", id = 629)
   void failedPartitionHandlingTxCompletion(GlobalTransaction globalTransaction, @Cause Throwable t);

//   @LogMessage(level = WARN)
//   @Message(value = "Another partition or topology changed for while completing partial completed transaction. Retrying later.", id = 630)
//   void topologyChangedPartitionHandlingTxCompletion();

   @Message(value = "Cross-site state transfer mode cannot be null.", id = 633)
   @Description("The mode attribute for cross-site state transfer configuration must have a value of AUTO or MANUAL. Modify the cache configuration with a valid value for the mode attribute.")
   CacheConfigurationException invalidXSiteStateTransferMode();

   @Message(value = "Cross-site automatic state transfer is not compatible with SYNC backup strategy.", id = 634)
   @Description("Automatic state transfer is not possible if the backup strategy for cross-site replication is synchronous. Modify the cache configuration and set the state transfer mode to MANUAL. Alternatively you can change the backup strategy to use asynchronous mode.")
   CacheConfigurationException autoXSiteStateTransferModeNotAvailableInSync();

   @LogMessage(level = WARN)
   @Message(value = "[%s] Failed to receive a response from any nodes. Automatic cross-site state transfer to site '%s' is not started.", id = 635)
   @Description("Before it starts automatic cross-site state transfer operations, the coordinator node checks all local nodes to determine if state transfer is necessary. This error occurs when the coordinator node gets an exception from one or more local nodes. Check that nodes in the cluster are online and operating as expected.")
   void unableToStartXSiteAutStateTransfer(String cacheName, String targetSite, @Cause Throwable t);

   @Message(value = "State transfer timeout (%s) must be greater than or equal to the remote timeout (%s)", id = 636)
   CacheConfigurationException invalidStateTransferTimeout(String stateTransferTimeout, String remoteTimeout);

   @Message(value = "Timeout waiting for topology %d, current topology is %d", id = 637)
   TimeoutException topologyTimeout(int expectedTopologyId, int currentTopologyId);

   @Message(value = "Timeout waiting for topology %d transaction data", id = 638)
   TimeoutException transactionDataTimeout(int expectedTopologyId);

//   @LogMessage(level = ERROR)
//   @Message(value = "Failed to send remove request to remote site(s). Reason: tombstone was lost. Key='%s'", id = 639)
//   void sendFailMissingTombstone(Object key);

   @LogMessage(level = WARN)
   @Message(value = "SingleFileStore has been deprecated and will be removed in a future version, replaced by SoftIndexFileStore", id = 640)
   void warnUsingDeprecatedSingleFileStore();

   @Message(value = "The transaction %s is already rolled back", id = 641)
   InvalidTransactionException transactionAlreadyRolledBack(GlobalTransaction gtx);

   @LogMessage(level = INFO)
   @Message(value = "Attempting to recover possibly corrupted data file %s", id = 642)
   void startRecoveringCorruptPersistenceData(String cacheName);

//   @LogMessage(level = INFO)
//   @Message(value = "'%s' persisted data successfully recovered %d entries.", id = 643)
//   void corruptDataSuccessfulMigrated(String cacheName, int entries);

   @Message(value = "Failed to recover '%s' persisted data.", id = 644)
   PersistenceException corruptDataMigrationFailed(String cacheName, @Cause Throwable cause);

   @Message(value = "Asynchronous cache modes, such as %s, cannot use SYNC touch mode for maximum idle expiration.", id = 645)
   CacheConfigurationException invalidTouchMode(CacheMode cacheMode);

   @Message(value = "capacityFactor must be positive", id = 646)
   IllegalArgumentException illegalCapacityFactor();

   @Message(value = "The configuration for internal cache '%s' cannot be modified", id = 647)
   IllegalArgumentException cannotUpdateInternalCache(String name);

   @Message(value = "Cache '%s' is non empty, cannot add store.", id = 648)
   PersistenceException cannotAddStore(String cacheName);

   @Message(value = "SingleFileStore does not support max-entries when segmented", id = 649)
   CacheConfigurationException segmentedSingleFileStoreDoesNotSupportMaxEntries();

   @Message(value = "Read invalid data in SingleFileStore file %s, please remove the file and retry", id = 650)
   PersistenceException invalidSingleFileStoreData(String path);

   @Message(value = "Max idle is not allowed while using a store without passivation", id = 651)
   CacheConfigurationException maxIdleNotAllowedWithoutPassivation();

   @LogMessage(level = WARN)
   @Message(value = "Max idle is not supported when using a store", id = 652)
   void maxIdleNotTestedWithPassivation();

   @LogMessage(level = WARN)
   @Message(value = "The '%s' attribute on the '%s' element has been deprecated. Please use the '%s' attribute instead", id = 653)
   void attributeDeprecatedUseOther(Enum<?> attr, Enum<?> element, Enum<?> other);

   @Message(value = "Problem encountered when preloading key %s!", id = 654)
   PersistenceException problemPreloadingKey(Object key, @Cause Throwable t);

   @Message(value = "Unable to convert text content to JSON: '%s'", id = 655)
   EncodingException invalidJson(String s);

   @Message(value = "The backup '%s' configuration 'failure-policy=%s' is not valid with an ASYNC backup strategy.", id = 656)
   @Description("Only the 'WARN' and 'IGNORE' failure policies are compatible with asynchronous backups for cross-site replication. Modify the backup configuration for the cache to change the failure policy or use the synchronous backup strategy.")
   CacheConfigurationException invalidPolicyWithAsyncStrategy(String remoteSite, BackupFailurePolicy policy);

   @Message(value = "The backup '%s' configuration 'failure-policy-class' is not compatible with 'failure-policy=%s'. Use 'failure-policy=\"CUSTOM\"'", id = 657)
   @Description("The backup configuration for the cache specifies the fully qualified class of a custom failure policy implementation. This is valid with the custom failure policy only. Change the cache configuration to use 'failure-policy=\"CUSTOM\"' or remove the failure policy class.")
   CacheConfigurationException failurePolicyClassNotCompatibleWith(String remoteSite, BackupFailurePolicy policy);

   @Message(value = "Initial state transfer timed out for cache %s on %s", id = 658)
   TimeoutException initialStateTransferTimeout(String cacheName, Address localAddress);

   @Message(value = "Component %s failed to start", id = 659)
   CacheConfigurationException componentFailedToStart(String componentName, @Cause Throwable t);

   @LogMessage(level = ERROR)
   @Message(value = "%s start failed, stopping any running components", id = 660)
   void startFailure(String registryName, @Cause Throwable t);

   @LogMessage(level = WARN)
   @Message(value = "'%s' has been deprecated with no replacement.", id = 661)
   void configDeprecated(Enum<?> element);

   @LogMessage(level = WARN)
   @Message(value = "Failed to transfer cross-site tombstones to %s for segments %s.", id = 662)
   @Description("Cross-site tombstones are metadata that ensure data consistency. This error indicates that it was not possible to replicate tombstones for some segments during normal operations. No action necessary.")
   void failedToTransferTombstones(Address requestor, IntSet segments, @Cause Throwable t);

   @Message(value = "Name must be less than 256 bytes, current name '%s' exceeds the size.", id = 663)
   CacheConfigurationException invalidNameSize(String name);

   @Message(value = "Invalid index startup mode: %s", id = 664)
   CacheConfigurationException invalidIndexStartUpMode(String invalidValue);

   @LogMessage(level = ERROR)
   @Message(value = "There was an error in submitted periodic task with %s, not rescheduling.", id = 665)
   void scheduledTaskEncounteredThrowable(Object identifier, @Cause Throwable t);

   @Message(value = "Transport clusterName cannot be null.", id = 666)
   CacheConfigurationException requireNonNullClusterName();

   @Message(value = "Transport node-name is not set.", id = 667)
   CacheConfigurationException requireNodeName();

   @Message(value = "Transport node-name must be present in raft-members: %s", id = 668)
   CacheConfigurationException nodeNameNotInRaftMembers(String members);

   @Message(value = "FORK protocol required on JGroups channel.", id = 669)
   IllegalArgumentException forkProtocolRequired();

   @Message(value = "Error creating fork channel for %s", id = 670)
   @LogMessage(level = ERROR)
   void errorCreatingForkChannel(String name, @Cause Throwable throwable);

   @Message(value = "RAFT protocol is not available. Reason: %s", id = 671)
   @LogMessage(level = WARN)
   void raftProtocolUnavailable(String reason);

   @Message(value = "RAFT protocol is available.", id = 672)
   @LogMessage(level = INFO)
   void raftProtocolAvailable();

   @Message(value = "Cannot persist RAFT data as global state is disabled", id = 673)
   CacheConfigurationException raftGlobalStateDisabled();

   @Message(value = "There was an error when resetting the SIFS index for cache %s", id = 674)
   PersistenceException issueEncounteredResettingIndex(String cacheName, @Cause Throwable t);

   @LogMessage(level = ERROR)
   @Message(value = "Caught exception while invoking a event logger listener!", id = 675)
   void failedInvokingEventLoggerListener(@Cause Throwable e);

   @LogMessage(level = WARN)
   @Message(value = "Store '%s'#isAvailable check threw Exception", id = 676)
   void storeIsAvailableCheckThrewException(@Cause Throwable e, String storeImpl);

   @LogMessage(level = WARN)
   @Message(value = "Store '%s'#isAvailable completed Exceptionally", id = 677)
   void storeIsAvailableCompletedExceptionally(@Cause Throwable e, String storeImpl);

   @LogMessage(level = WARN)
   @Message(value = "Persistence is unavailable because of store %s", id = 678)
   void persistenceUnavailable(String storeImpl);

   @LogMessage(level = INFO)
   @Message(value = "Persistence is now available", id = 679)
   void persistenceAvailable();

   @Message(value = "Expiration (Max idle or Lifespan) is not allowed while using a store '%s' that does not support expiration, unless it is configured as read only", id = 680)
   CacheConfigurationException expirationNotAllowedWhenStoreDoesNotSupport(String storeImpl);

   @Message(value = "Missing required property '%s' for attribute '%s' at %s", id = 681)
   CacheConfigurationException missingRequiredProperty(String property, String name, Location location);

   @Message(value = "Attribute '%2$s' of element '%1$s' has an illegal value '%3$s' at %4$s: %5$s", id = 686)
   CacheConfigurationException invalidAttributeValue(String element, String attribute, String value, Location location, String message);

   @Message(value = "Attribute '%2$s' of element '%1$s' has an illegal value '%3$s' at %5$s. Expecting one of %4$s.", id = 687)
   CacheConfigurationException invalidAttributeEnumValue(String element, String attribute, String value, String world, Location location);

   @LogMessage(level = WARN)
   @Message(value = "Attribute '%s' of element '%s' has been deprecated since schema version %d.%d. Refer to the upgrade guide", id = 688)
   void attributeDeprecated(String name, String element, int major, int minor);

   @LogMessage(level = WARN)
   @Message(value = "Recovering cache '%s' but there are missing members, known members %s of a total of %s", id = 689)
   void recoverFromStateMissingMembers(String cacheName, List<Address> members, int total);

   MissingMembersException recoverFromStateMissingMembers(String cacheName, List<Address> members, String total);

   @LogMessage(level = DEBUG)
   @Message(value = "We cannot find a configuration for the cache '%s' in the available configurations: '%s'. " +
         "This cache has been probably removed by another thread. Skip to writing it.", id = 690)
   void configurationNotFound(String cacheName, Collection<String> definedConfigurations);

   @Message(value = "Indexed entity name must not be null or empty", id = 691)
   CacheConfigurationException indexedEntityNameMissing();

   @LogMessage(level = INFO)
   @Message(value = "Flushed ACL Cache", id = 692)
   void flushedACLCache();

   @Message(value = "Dangling lock file '%s' in persistent global state, probably left behind by an unclean shutdown. ", id = 693)
   CacheConfigurationException globalStateLockFilePresent(FileSystemLock lockFile);

   @Message(value = "Cache '%s' has number of owners %d but is missing too many members (%d/%d) to reinstall topology", id = 694)
   MissingMembersException missingTooManyMembers(String cacheName, int owners, int missing, int total);

   @Message(value = "Query module not found. Add infinispan-query or infinispan-query-core to the classpath.", id = 695)
   CacheException queryNotSupported();

   @Message(value = "Simple caches do not support queries. Use standard caches for querying.", id = 696)
   CacheException querySimpleCacheNotSupported();

   @LogMessage(level = WARN)
   @Message(value = "Failed to register metrics with id %s. Reason: %s", id = 697)
   void metricRegistrationFailed(String id, String reason);

   @Message(value = "Cannot handle cross-site request from site '%s'. CacheManager isn't started yet.", id = 698)
   CrossSiteIllegalLifecycleStateException xsiteCacheManagerDoesNotAllowInvocations(String origin);

   @Message(value = "Tracing collector endpoint '%s' is not valid.", id = 699)
   CacheConfigurationException invalidTracingCollectorEndpoint(String collectorEndpoint, @Cause Throwable e);

   @Message(value = "Cannot use id %d for commands, as it is already in use by %s", id = 700)
   IllegalArgumentException commandIdAlreadyInUse(byte id, String name);

   @Message(value = "Security container cannot be enabled/disabled at cache level, since it is configured globally.", id = 701)
   CacheConfigurationException securityCacheTracing();

   @Message(value = "The alias '%s' is already being used by cache '%s'", id = 702)
   CacheConfigurationException duplicateAliasName(String alias, String cacheName);

   @LogMessage(level = INFO)
   @Message(value = "Received new cross-site event, site(s) %s: %s", id = 703)
   @Description("A cluster has either joined or left the global cluster view.")
   void crossSiteViewEvent(String action, String sites);

   @Message(value = "%s marshaller implementation not overridden in SerializationContext", id = 704)
   IllegalStateException marshallerNotOverridden(String className);
}

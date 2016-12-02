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
import java.net.URL;
import java.nio.channels.FileChannel;
import java.security.Permission;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanRegistrationException;
import javax.management.ObjectName;
import javax.naming.NamingException;
import javax.transaction.Synchronization;
import javax.transaction.TransactionManager;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.xml.namespace.QName;

import org.infinispan.IllegalLifecycleStateException;
import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.CacheListenerException;
import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.commons.util.TypedProperties;
import org.infinispan.jmx.JmxDomainConflictException;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.partitionhandling.AvailabilityException;
import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.persistence.support.SingletonCacheWriter;
import org.infinispan.remoting.RemoteException;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.jgroups.SuspectException;
import org.infinispan.topology.CacheJoinException;
import org.infinispan.topology.CacheTopology;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.transaction.impl.LocalTransaction;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.transaction.xa.recovery.RecoveryAwareRemoteTransaction;
import org.infinispan.transaction.xa.recovery.RecoveryAwareTransaction;
import org.infinispan.util.concurrent.TimeoutException;
import org.jboss.logging.BasicLogger;
import org.jboss.logging.annotations.Cause;
import org.jboss.logging.annotations.LogMessage;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageLogger;
import org.jgroups.View;

/**
 * Infinispan's log abstraction layer on top of JBoss Logging.
 * <p/>
 * It contains explicit methods for all INFO or above levels so that they can
 * be internationalized. For the core module, message ids ranging from 0001
 * to 0900 inclusively have been reserved.
 * <p/>
 * <code> Log log = LogFactory.getLog( getClass() ); </code> The above will get
 * you an instance of <tt>Log</tt>, which can be used to generate log messages
 * either via JBoss Logging which then can delegate to Log4J (if the libraries
 * are present) or (if not) the built-in JDK logger.
 * <p/>
 * In addition to the 6 log levels available, this framework also supports
 * parameter interpolation, similar to the JDKs {@link String#format(String, Object...)}
 * method. What this means is, that the following block:
 * <code> if (log.isTraceEnabled()) { log.trace("This is a message " + message + " and some other value is " + value); }
 * </code>
 * <p/>
 * ... could be replaced with ...
 * <p/>
 * <code> if (log.isTraceEnabled()) log.tracef("This is a message %s and some other value is %s", message, value);
 * </code>
 * <p/>
 * This greatly enhances code readability.
 * <p/>
 * If you are passing a <tt>Throwable</tt>, note that this should be passed in
 * <i>before</i> the vararg parameter list.
 * <p/>
 *
 * @author Manik Surtani
 * @since 4.0
 * @private
 */
@MessageLogger(projectCode = "ISPN")
public interface Log extends BasicLogger {

   @LogMessage(level = WARN)
   @Message(value = "Unable to load %s from cache loader", id = 1)
   void unableToLoadFromCacheLoader(Object key, @Cause PersistenceException cle);

   @LogMessage(level = WARN)
   @Message(value = "Field %s not found!!", id = 2)
   void fieldNotFound(String fieldName);

   @LogMessage(level = WARN)
   @Message(value = "Property %s could not be replaced as intended!", id = 3)
   void propertyCouldNotBeReplaced(String line);

   @LogMessage(level = WARN)
   @Message(value = "Unexpected error reading properties", id = 4)
   void errorReadingProperties(@Cause IOException e);

   @LogMessage(level = WARN)
   @Message(value = "Detected write skew on key [%s]. Another process has changed the entry since we last read it! Unable to copy entry for update.", id = 5)
   void unableToCopyEntryForUpdate(Object key);

   @LogMessage(level = WARN)
   @Message(value = "Failed remote execution on node %s", id = 6)
   void remoteExecutionFailed(Address address, @Cause Throwable t);

   @LogMessage(level = WARN)
   @Message(value = "Failed local execution ", id = 7)
   void localExecutionFailed(@Cause Throwable t);

   @LogMessage(level = WARN)
   @Message(value = "Can not select %s random members for %s", id = 8)
   void cannotSelectRandomMembers(int numNeeded, List<Address> members);

   @LogMessage(level = INFO)
   @Message(value = "DistributionManager not yet joined the cluster. Cannot do anything about other concurrent joiners.", id = 14)
   void distributionManagerNotJoined();

   @LogMessage(level = WARN)
   @Message(value = "DistributionManager not started after waiting up to 5 minutes! Not rehashing!", id = 15)
   void distributionManagerNotStarted();

   @LogMessage(level = WARN)
   @Message(value = "Problem %s encountered when applying state for key %s!", id = 16)
   void problemApplyingStateForKey(String msg, Object key, @Cause Throwable t);

   @LogMessage(level = WARN)
   @Message(value = "Unable to apply prepare %s", id = 18)
   void unableToApplyPrepare(PrepareCommand pc, @Cause Throwable t);

   @LogMessage(level = INFO)
   @Message(value = "Couldn't acquire shared lock", id = 19)
   void couldNotAcquireSharedLock();

   @LogMessage(level = WARN)
   @Message(value = "Expected just one response; got %s", id = 21)
   void expectedJustOneResponse(Map<Address, Response> lr);

   @LogMessage(level = INFO)
   @Message(value = "wakeUpInterval is <= 0, not starting expired purge thread", id = 25)
   void notStartingEvictionThread();

   @LogMessage(level = WARN)
   @Message(value = "Caught exception purging data container!", id = 26)
   void exceptionPurgingDataContainer(@Cause Exception e);

   @LogMessage(level = WARN)
   @Message(value = "Could not acquire lock for eviction of %s", id = 27)
   void couldNotAcquireLockForEviction(Object key, @Cause Exception e);

   @LogMessage(level = WARN)
   @Message(value = "Unable to passivate entry under %s", id = 28)
   void unableToPassivateEntry(Object key, @Cause Exception e);

   @LogMessage(level = INFO)
   @Message(value = "Passivating all entries to disk", id = 29)
   void passivatingAllEntries();

   @LogMessage(level = INFO)
   @Message(value = "Passivated %d entries in %s", id = 30)
   void passivatedEntries(int numEntries, String duration);

   @LogMessage(level = TRACE)
   @Message(value = "MBeans were successfully registered to the platform MBean server.", id = 31)
   void mbeansSuccessfullyRegistered();

   @LogMessage(level = WARN)
   @Message(value = "Problems un-registering MBeans", id = 32)
   void problemsUnregisteringMBeans(@Cause Exception e);

   @LogMessage(level = WARN)
   @Message(value = "Unable to unregister Cache MBeans with pattern %s", id = 33)
   void unableToUnregisterMBeanWithPattern(String pattern, @Cause MBeanRegistrationException e);

   @Message(value = "There's already a JMX MBean instance %s already registered under " +
         "'%s' JMX domain. If you want to allow multiple instances configured " +
         "with same JMX domain enable 'allowDuplicateDomains' attribute in " +
         "'globalJmxStatistics' config element", id = 34)
   JmxDomainConflictException jmxMBeanAlreadyRegistered(String mBeanName, String jmxDomain);

   @LogMessage(level = WARN)
   @Message(value = "Could not reflect field description of this class. Was it removed?", id = 35)
   void couldNotFindDescriptionField();

   @LogMessage(level = WARN)
   @Message(value = "Did not find attribute %s", id = 36)
   void couldNotFindAttribute(String name);

   @LogMessage(level = WARN)
   @Message(value = "Failed to update attribute name %s with value %s", id = 37)
   void failedToUpdateAttribute(String name, Object value);

   @LogMessage(level = WARN)
   @Message(value = "Method name %s doesn't start with \"get\", \"set\", or \"is\" " +
         "but is annotated with @ManagedAttribute: will be ignored", id = 38)
   void ignoringManagedAttribute(String methodName);

   @LogMessage(level = WARN)
   @Message(value = "Method %s must have a valid return type and zero parameters", id = 39)
   void invalidManagedAttributeMethod(String methodName);

   @LogMessage(level = WARN)
   @Message(value = "Not adding annotated method %s since we already have read attribute", id = 40)
   void readManagedAttributeAlreadyPresent(Method m);

   @LogMessage(level = WARN)
   @Message(value = "Not adding annotated method %s since we already have writable attribute", id = 41)
   void writeManagedAttributeAlreadyPresent(String methodName);

   @LogMessage(level = WARN)
   @Message(value = "Did not find queried attribute with name %s", id = 42)
   void queriedAttributeNotFound(String attributeName);

   @LogMessage(level = WARN)
   @Message(value = "Exception while writing value for attribute %s", id = 43)
   void errorWritingValueForAttribute(String attributeName, @Cause Exception e);

   @LogMessage(level = WARN)
   @Message(value = "Could not invoke set on attribute %s with value %s", id = 44)
   void couldNotInvokeSetOnAttribute(String attributeName, Object value);

   @LogMessage(level = ERROR)
   @Message(value = "Problems encountered while purging expired", id = 45)
   void problemPurgingExpired(@Cause Exception e);

   @LogMessage(level = ERROR)
   @Message(value = "Unknown responses from remote cache: %s", id = 46)
   void unknownResponsesFromRemoteCache(Collection<Response> responses);

   @LogMessage(level = ERROR)
   @Message(value = "Error while doing remote call", id = 47)
   void errorDoingRemoteCall(@Cause Exception e);

   @LogMessage(level = ERROR)
   @Message(value = "Interrupted or timeout while waiting for AsyncCacheWriter worker threads to push all state to the decorated store", id = 48)
   void interruptedWaitingAsyncStorePush(@Cause InterruptedException e);

   @LogMessage(level = ERROR)
   @Message(value = "Unexpected error", id = 51)
   void unexpectedErrorInAsyncProcessor(@Cause Throwable t);

   @LogMessage(level = ERROR)
   @Message(value = "Interrupted on acquireLock for %d milliseconds!", id = 52)
   void interruptedAcquiringLock(long ms, @Cause InterruptedException e);

   @LogMessage(level = WARN)
   @Message(value = "Unable to process some async modifications after %d retries!", id = 53)
   void unableToProcessAsyncModifications(int retries);

   @LogMessage(level = ERROR)
   @Message(value = "Unexpected error in AsyncStoreCoordinator thread. AsyncCacheWriter is dead!", id = 55)
   void unexpectedErrorInAsyncStoreCoordinator(@Cause Throwable t);

   @LogMessage(level = ERROR)
   @Message(value = "Exception reported changing cache active status", id = 58)
   void errorChangingSingletonStoreStatus(@Cause SingletonCacheWriter.PushStateException e);

   @LogMessage(level = WARN)
   @Message(value = "Had problems removing file %s", id = 59)
   void problemsRemovingFile(File f);

   @LogMessage(level = WARN)
   @Message(value = "Problems purging file %s", id = 60)
   void problemsPurgingFile(File buckedFile, @Cause PersistenceException e);

   @LogMessage(level = WARN)
   @Message(value = "Unable to acquire global lock to purge cache store", id = 61)
   void unableToAcquireLockToPurgeStore();

   @LogMessage(level = ERROR)
   @Message(value = "Error while reading from file: %s", id = 62)
   void errorReadingFromFile(File f, @Cause Exception e);

   @LogMessage(level = WARN)
   @Message(value = "Problems creating the directory: %s", id = 64)
   void problemsCreatingDirectory(File dir);

   @LogMessage(level = ERROR)
   @Message(value = "Exception while marshalling object: %s", id = 65)
   void errorMarshallingObject(@Cause Throwable ioe, Object obj);

   @LogMessage(level = ERROR)
   @Message(value = "Unable to read version id from first two bytes of stream, barfing.", id = 66)
   void unableToReadVersionId();

   @LogMessage(level = INFO)
   @Message(value = "Will try and wait for the cache %s to start", id = 67)
   void waitForCacheToStart(String cacheName);

   @LogMessage(level = INFO)
   @Message(value = "Cache named %s does not exist on this cache manager!", id = 68)
   void namedCacheDoesNotExist(String cacheName);

   @LogMessage(level = WARN)
   @Message(value = "Caught exception when handling command %s", id = 71)
   void exceptionHandlingCommand(ReplicableCommand cmd, @Cause Throwable t);

   @LogMessage(level = ERROR)
   @Message(value = "Unexpected error while replicating", id = 73)
   void unexpectedErrorReplicating(@Cause Throwable t);

   @LogMessage(level = ERROR)
   @Message(value = "Message or message buffer is null or empty.", id = 77)
   void msgOrMsgBufferEmpty();

   @LogMessage(level = INFO)
   @Message(value = "Starting JGroups channel %s", id = 78)
   void startingJGroupsChannel(String cluster);

   @LogMessage(level = INFO)
   @Message(value = "Channel %s local address is %s, physical addresses are %s", id = 79)
   void localAndPhysicalAddress(String cluster, Address address, List<Address> physicalAddresses);

   @LogMessage(level = INFO)
   @Message(value = "Disconnecting JGroups channel %s", id = 80)
   void disconnectJGroups(String cluster);

   @LogMessage(level = ERROR)
   @Message(value = "Problem closing channel %s; setting it to null", id = 81)
   void problemClosingChannel(@Cause Exception e, String cluster);

   @LogMessage(level = INFO)
   @Message(value = "Stopping the RpcDispatcher for channel %s", id = 82)
   void stoppingRpcDispatcher(String cluster);

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

   @LogMessage(level = WARN)
   @Message(value = "Channel not set up properly!", id = 92)
   void channelNotSetUp();

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

   @LogMessage(level = ERROR)
   @Message(value = "Unprocessed Transaction Log Entries! = %d", id = 99)
   void unprocessedTxLogEntries(int size);

   @LogMessage(level = WARN)
   @Message(value = "Stopping, but there are %s local transactions and %s remote transactions that did not finish in time.", id = 100)
   void unfinishedTransactionsRemain(int localTransactions, int remoteTransactions);

   @LogMessage(level = WARN)
   @Message(value = "Failed synchronization registration", id = 101)
   void failedSynchronizationRegistration(@Cause Exception e);

   @LogMessage(level = WARN)
   @Message(value = "Unable to roll back global transaction %s", id = 102)
   void unableToRollbackGlobalTx(GlobalTransaction gtx, @Cause Throwable e);

   @LogMessage(level = ERROR)
   @Message(value = "A remote transaction with the given id was already registered!!!", id = 103)
   void remoteTxAlreadyRegistered();

   @LogMessage(level = WARN)
   @Message(value = "Falling back to DummyTransactionManager from Infinispan", id = 104)
   void fallingBackToDummyTm();

   @LogMessage(level = ERROR)
   @Message(value = "Failed creating initial JNDI context", id = 105)
   void failedToCreateInitialCtx(@Cause NamingException e);

   @LogMessage(level = ERROR)
   @Message(value = "Found WebSphere TransactionManager factory class [%s], but " +
         "couldn't invoke its static 'getTransactionManager' method", id = 106)
   void unableToInvokeWebsphereStaticGetTmMethod(@Cause Exception ex, String className);

   @LogMessage(level = INFO)
   @Message(value = "Retrieving transaction manager %s", id = 107)
   void retrievingTm(TransactionManager tm);

   @LogMessage(level = ERROR)
   @Message(value = "Error enlisting resource", id = 108)
   void errorEnlistingResource(@Cause XAException e);

   @LogMessage(level = ERROR)
   @Message(value = "beforeCompletion() failed for %s", id = 109)
   void beforeCompletionFailed(Synchronization s, @Cause Throwable t);

   @LogMessage(level = ERROR)
   @Message(value = "Unexpected error from resource manager!", id = 110)
   void unexpectedErrorFromResourceManager(@Cause Throwable t);

   @LogMessage(level = ERROR)
   @Message(value = "afterCompletion() failed for %s", id = 111)
   void afterCompletionFailed(Synchronization s, @Cause Throwable t);

   @LogMessage(level = WARN)
   @Message(value = "exception while committing", id = 112)
   void errorCommittingTx(@Cause XAException e);

   @LogMessage(level = ERROR)
   @Message(value = "Unbinding of DummyTransactionManager failed", id = 113)
   void unbindingDummyTmFailed(@Cause NamingException e);

   @LogMessage(level = ERROR)
   @Message(value = "Unsupported combination (dldEnabled, recoveryEnabled, xa) = (%s, %s, %s)", id = 114)
   void unsupportedTransactionConfiguration(boolean dldEnabled, boolean recoveryEnabled, boolean xa);

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

   @LogMessage(level = WARN)
   @Message(value = "Could not load module at URL %s", id = 118)
   void couldNotLoadModuleAtUrl(URL url, @Cause Exception ex);

   @LogMessage(level = WARN)
   @Message(value = "Module %s loaded, but could not be initialized", id = 119)
   void couldNotInitializeModule(Object key, @Cause Exception ex);

   @LogMessage(level = WARN)
   @Message(value = "Invocation of %s threw an exception %s. Exception is ignored.", id = 120)
   void ignoringException(String methodName, String exceptionName, @Cause Throwable t);

   @LogMessage(level = ERROR)
   @Message(value = "Unable to set value!", id = 121)
   void unableToSetValue(@Cause Exception e);

   @LogMessage(level = WARN)
   @Message(value = "Unable to convert string property [%s] to an int! Using default value of %d", id = 122)
   void unableToConvertStringPropertyToInt(String value, int defaultValue);

   @LogMessage(level = WARN)
   @Message(value = "Unable to convert string property [%s] to a long! Using default value of %d", id = 123)
   void unableToConvertStringPropertyToLong(String value, long defaultValue);

   @LogMessage(level = WARN)
   @Message(value = "Unable to convert string property [%s] to a boolean! Using default value of %b", id = 124)
   void unableToConvertStringPropertyToBoolean(String value, boolean defaultValue);

   @LogMessage(level = WARN)
   @Message(value = "Unable to invoke getter %s on Configuration.class!", id = 125)
   void unableToInvokeGetterOnConfiguration(Method getter, @Cause Exception e);

   @LogMessage(level = WARN)
   @Message(value = "Attempted to stop() from FAILED state, but caught exception; try calling destroy()", id = 126)
   void failedToCallStopAfterFailure(@Cause Throwable t);

   @LogMessage(level = WARN)
   @Message(value = "Needed to call stop() before destroying but stop() threw exception. Proceeding to destroy", id = 127)
   void stopBeforeDestroyFailed(@Cause CacheException e);

   @LogMessage(level = INFO)
   @Message(value = "Infinispan version: %s", id = 128)
   void version(String version);

   @LogMessage(level = WARN)
   @Message(value = "Received a remote call but the cache is not in STARTED state - ignoring call.", id = 129)
   void cacheNotStarted();

   @LogMessage(level = ERROR)
   @Message(value = "Caught exception! Aborting join.", id = 130)
   void abortingJoin(@Cause Exception e);

   @LogMessage(level = INFO)
   @Message(value = "%s completed join rehash in %s!", id = 131)
   void joinRehashCompleted(Address self, String duration);

   @LogMessage(level = INFO)
   @Message(value = "%s aborted join rehash after %s!", id = 132)
   void joinRehashAborted(Address self, String duration);

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
   @Message(value = "Error executing command %s, writing keys %s", id = 136)
   void executionError(String commandType, String affectedKeys, @Cause Throwable t);

   @LogMessage(level = INFO)
   @Message(value = "Failed invalidating remote cache", id = 137)
   void failedInvalidatingRemoteCache(@Cause Exception e);

   @LogMessage(level = INFO)
   @Message(value = "Could not register object with name: %s", id = 138)
   void couldNotRegisterObjectName(ObjectName objectName, @Cause InstanceAlreadyExistsException e);

   @LogMessage(level = WARN)
   @Message(value = "Infinispan configuration schema could not be resolved locally nor fetched from URL. Local path=%s, schema path=%s, schema URL=%s", id = 139)
   void couldNotResolveConfigurationSchema(String localPath, String schemaPath, String schemaURL);

   @LogMessage(level = WARN)
   @Message(value = "Lazy deserialization configuration is deprecated, please use storeAsBinary instead", id = 140)
   void lazyDeserializationDeprecated();

   @LogMessage(level = WARN)
   @Message(value = "Could not rollback prepared 1PC transaction. This transaction will be rolled back by the recovery process, if enabled. Transaction: %s", id = 141)
   void couldNotRollbackPrepared1PcTransaction(LocalTransaction localTransaction, @Cause Throwable e1);

   @LogMessage(level = WARN)
   @Message(value = "Received a key that doesn't map to this node: %s, mapped to %s", id = 143)
   void keyDoesNotMapToLocalNode(Object key, Collection<Address> nodes);

   @LogMessage(level = WARN)
   @Message(value = "Failed loading value for key %s from cache store", id = 144)
   void failedLoadingValueFromCacheStore(Object key, @Cause Exception e);

   @LogMessage(level = ERROR)
   @Message(value = "Error invalidating keys from L1 after rehash", id = 147)
   void failedToInvalidateKeys(@Cause Exception e);

   @LogMessage(level = WARN)
   @Message(value = "Invalid %s value of %s. It can not be higher than %s which is %s", id = 148)
   void invalidTimeoutValue(Object configName1, Object value1, Object configName2, Object value2);

   @LogMessage(level = WARN)
   @Message(value = "Fetch persistent state and purge on startup are both disabled, cache may contain stale entries on startup", id = 149)
   void staleEntriesWithoutFetchPersistentStateOrPurgeOnStartup();

   @LogMessage(level = FATAL)
   @Message(value = "Rehash command received on non-distributed cache. All the nodes in the cluster should be using the same configuration.", id = 150)
   void rehashCommandReceivedOnNonDistributedCache();

   @LogMessage(level = ERROR)
   @Message(value = "Error flushing to file: %s", id = 151)
   void errorFlushingToFileChannel(FileChannel f, @Cause Exception e);

   @LogMessage(level = INFO)
   @Message(value = "Passivation configured without an eviction policy being selected. " +
      "Only manually evicted entities will be passivated.", id = 152)
   void passivationWithoutEviction();

   // Warning ISPN000153 removed as per ISPN-2554

   @LogMessage(level = ERROR)
   @Message(value = "Unable to unlock keys %2$s for transaction %1$s after they were rebalanced off node %3$s", id = 154)
   void unableToUnlockRebalancedKeys(GlobalTransaction gtx, List<Object> keys, Address self, @Cause Throwable t);

   @LogMessage(level = WARN)
   @Message(value = "Unblocking transactions failed", id = 159)
   void errorUnblockingTransactions(@Cause Exception e);

   @LogMessage(level = WARN)
   @Message(value = "Could not complete injected transaction.", id = 160)
   void couldNotCompleteInjectedTransaction(@Cause Throwable t);

   @LogMessage(level = INFO)
   @Message(value = "Using a batchMode transaction manager", id = 161)
   void usingBatchModeTransactionManager();

   @LogMessage(level = INFO)
   @Message(value = "Could not instantiate transaction manager", id = 162)
   void couldNotInstantiateTransactionManager(@Cause Exception e);

   @LogMessage(level = WARN)
   @Message(value = "FileCacheStore ignored an unexpected file %2$s in path %1$s. The store path should be dedicated!", id = 163)
   void cacheLoaderIgnoringUnexpectedFile(Object parentPath, String name);

   @LogMessage(level = ERROR)
   @Message(value = "Rolling back to cache view %d, but last committed view is %d", id = 164)
   void cacheViewRollbackIdMismatch(int committedViewId, int committedView);

   @LogMessage(level = INFO)
   @Message(value = "Strict peer-to-peer is enabled but the JGroups channel was started externally - this is very likely to result in RPC timeout errors on startup", id = 171)
   void warnStrictPeerToPeerWithInjectedChannel();

   @LogMessage(level = ERROR)
   @Message(value = "Custom interceptor %s has used @Inject, @Start or @Stop. These methods will not be processed. Please extend org.infinispan.interceptors.base.BaseCustomInterceptor instead, and your custom interceptor will have access to a cache and cacheManager.  Override stop() and start() for lifecycle methods.", id = 173)
   void customInterceptorExpectsInjection(String customInterceptorFQCN);

   @LogMessage(level = WARN)
   @Message(value = "Unexpected error reading configuration", id = 174)
   void errorReadingConfiguration(@Cause Exception e);

   @LogMessage(level = WARN)
   @Message(value = "Unexpected error closing resource", id = 175)
   void failedToCloseResource(@Cause Throwable e);

   @LogMessage(level = WARN)
   @Message(value = "The 'wakeUpInterval' attribute of the 'eviction' configuration XML element is deprecated. Setting the 'wakeUpInterval' attribute of the 'expiration' configuration XML element to %d instead", id = 176)
   void evictionWakeUpIntervalDeprecated(Long wakeUpInterval);

   @LogMessage(level = WARN)
   @Message(value = "%s has been deprecated as a synonym for %s. Use one of %s instead", id = 177)
   void randomCacheModeSynonymsDeprecated(String candidate, String mode, List<String> synonyms);

   @LogMessage(level = WARN)
   @Message(value = "stateRetrieval's 'alwaysProvideInMemoryState' attribute is no longer in use, " +
         "instead please make sure all instances of this named cache in the cluster have 'fetchInMemoryState' attribute enabled", id = 178)
   void alwaysProvideInMemoryStateDeprecated();

   @LogMessage(level = WARN)
   @Message(value = "stateRetrieval's 'initialRetryWaitTime' attribute is no longer in use.", id = 179)
   void initialRetryWaitTimeDeprecated();

   @LogMessage(level = WARN)
   @Message(value = "stateRetrieval's 'logFlushTimeout' attribute is no longer in use.", id = 180)
   void logFlushTimeoutDeprecated();

   @LogMessage(level = WARN)
   @Message(value = "stateRetrieval's 'maxProgressingLogWrites' attribute is no longer in use.", id = 181)
   void maxProgressingLogWritesDeprecated();

   @LogMessage(level = WARN)
   @Message(value = "stateRetrieval's 'numRetries' attribute is no longer in use.", id = 182)
   void numRetriesDeprecated();

   @LogMessage(level = WARN)
   @Message(value = "stateRetrieval's 'retryWaitTimeIncreaseFactor' attribute is no longer in use.", id = 183)
   void retryWaitTimeIncreaseFactorDeprecated();

   @LogMessage(level = INFO)
   @Message(value = "The stateRetrieval configuration element has been deprecated, " +
         "we're assuming you meant stateTransfer. Please see XML schema for more information.", id = 184)
   void stateRetrievalConfigurationDeprecated();

   @LogMessage(level = INFO)
   @Message(value = "hash's 'rehashEnabled' attribute has been deprecated. Please use stateTransfer.fetchInMemoryState instead", id = 185)
   void hashRehashEnabledDeprecated();

   @LogMessage(level = INFO)
   @Message(value = "hash's 'rehashRpcTimeout' attribute has been deprecated. Please use stateTransfer.timeout instead", id = 186)
   void hashRehashRpcTimeoutDeprecated();

   @LogMessage(level = INFO)
   @Message(value = "hash's 'rehashWait' attribute has been deprecated. Please use stateTransfer.timeout instead", id = 187)
   void hashRehashWaitDeprecated();

   @LogMessage(level = ERROR)
   @Message(value = "Error while processing a commit in a two-phase transaction", id = 188)
   void errorProcessing2pcCommitCommand(@Cause Throwable e);

   @LogMessage(level = WARN)
   @Message(value = "While stopping a cache or cache manager, one of its components failed to stop", id = 189)
   void componentFailedToStop(@Cause Throwable e);

   @LogMessage(level = WARN)
   @Message(value = "Use of the 'loader' element to configure a store is deprecated, please use the 'store' element instead", id = 190)
   void deprecatedLoaderAsStoreConfiguration();

   @LogMessage(level = DEBUG)
   @Message(value = "When indexing locally a cache with shared cache loader, preload must be enabled", id = 191)
   void localIndexingWithSharedCacheLoaderRequiresPreload();

   @LogMessage(level = WARN)
   @Message(value = "hash's 'numVirtualNodes' attribute has been deprecated. Please use hash.numSegments instead", id = 192)
   void hashNumVirtualNodesDeprecated();

   @LogMessage(level = WARN)
   @Message(value = "hash's 'consistentHash' attribute has been deprecated. Please use hash.consistentHashFactory instead", id = 193)
   void consistentHashDeprecated();

   @LogMessage(level = WARN)
   @Message(value = "Failed loading keys from cache store", id = 194)
   void failedLoadingKeysFromCacheStore(@Cause Exception e);

   @LogMessage(level = ERROR)
   @Message(value = "Error during rebalance for cache %s on node %s", id = 195)
   void rebalanceError(String cacheName, Address node, @Cause Throwable cause);

   @LogMessage(level = ERROR)
   @Message(value = "Failed to recover cluster state after the current node became the coordinator (or after merge)", id = 196)
   void failedToRecoverClusterState(@Cause Throwable cause);

   @LogMessage(level = WARN)
   @Message(value = "Error updating cluster member list", id = 197)
   void errorUpdatingMembersList(@Cause Throwable cause);

   @LogMessage(level = INFO)
   @Message(value = "Unable to register MBeans for default cache", id = 198)
   void unableToRegisterMBeans();

   @LogMessage(level = INFO)
   @Message(value = "Unable to register MBeans for named cache %s", id = 199)
   void unableToRegisterMBeans(String cacheName);

   @LogMessage(level = INFO)
   @Message(value = "Unable to register MBeans for cache manager", id = 200)
   void unableToRegisterCacheManagerMBeans();

   @LogMessage(level = TRACE)
   @Message(value = "This cache is configured to backup to its own site (%s).", id = 201)
   void cacheBackupsDataToSameSite(String siteName);

   @LogMessage(level = WARN)
   @Message(value = "Problems backing up data for cache %s to site %s: %s", id = 202)
   void warnXsiteBackupFailed(String cacheName, String key, Object value);

   @LogMessage(level = WARN)
   @Message(value = "The rollback request for tx %s cannot be processed by the cache %s as this cache is not transactional!", id=203)
   void cannotRespondToRollback(GlobalTransaction globalTransaction, String cacheName);

   @LogMessage(level = WARN)
   @Message(value = "The commit request for tx %s cannot be processed by the cache %s as this cache is not transactional!", id=204)
   void cannotRespondToCommit(GlobalTransaction globalTransaction, String cacheName);

   @LogMessage(level = WARN)
   @Message(value = "Trying to bring back an non-existent site (%s)!", id=205)
   void tryingToBringOnlineNonexistentSite(String siteName);

   @LogMessage(level = WARN)
   @Message(value = "Could not execute cancellation command locally", id=206)
   void couldNotExecuteCancellationLocally(@Cause Throwable e);

   @LogMessage(level = WARN)
   @Message(value = "Could not interrupt as no thread found for command uuid %s", id=207)
   void couldNotInterruptThread(UUID id);

   @LogMessage(level = ERROR)
   @Message(value = "No live owners found for segment %d of cache %s. Current owners are:  %s. Faulty owners: %s", id=208)
   void noLiveOwnersFoundForSegment(int segmentId, String cacheName, Collection<Address> owners, Collection<Address> faultySources);

   @LogMessage(level = WARN)
   @Message(value = "Failed to retrieve transactions for segments %s of cache %s from node %s", id=209)
   void failedToRetrieveTransactionsForSegments(Collection<Integer> segments, String cacheName, Address source, @Cause Exception e);

   @LogMessage(level = WARN)
   @Message(value = "Failed to request segments %s of cache %s from node %s (node will not be retried)", id=210)
   void failedToRequestSegments(Collection<Integer> segments, String cacheName, Address source, @Cause Throwable e);

   @LogMessage(level = ERROR)
   @Message(value = "Unable to load %s from any of the following classloaders: %s", id=213)
   void unableToLoadClass(String classname, String classloaders, @Cause Throwable cause);

   @LogMessage(level = WARN)
   @Message(value = "Unable to remove entry under %s from cache store after activation", id = 214)
   void unableToRemoveEntryAfterActivation(Object key, @Cause Exception e);

   @Message(value = "Unknown migrator %s", id=215)
   Exception unknownMigrator(String migratorName);

   @LogMessage(level = INFO)
   @Message(value = "%d entries migrated to cache %s in %s", id = 216)
   void entriesMigrated(long count, String name, String prettyTime);

   @Message(value = "Received exception from %s, see cause for remote stack trace", id = 217)
   RemoteException remoteException(Address sender, @Cause Throwable t);

   @LogMessage(level = INFO)
   @Message(value = "Timeout while waiting for the transaction validation. The command will not be processed. " +
         "Transaction is %s", id = 218)
   void timeoutWaitingUntilTransactionPrepared(String globalTx);

   @LogMessage(level = WARN)
   @Message(value = "Shutdown while handling command %s", id = 219)
   void shutdownHandlingCommand(ReplicableCommand command);

   @LogMessage(level = WARN)
   @Message(value = "Problems un-marshalling remote command from byte buffer", id = 220)
   void errorUnMarshallingCommand(@Cause Throwable throwable);

   @LogMessage(level = WARN)
   @Message(value = "Unknown response value [%s]. Expected [%s]", id = 221)
   void unexpectedResponse(String actual, String expected);

   @Message(value = "Custom interceptor missing class", id = 222)
   CacheConfigurationException customInterceptorMissingClass();

   @LogMessage(level = WARN)
   @Message(value = "Custom interceptor '%s' does not extend BaseCustomInterceptor, which is recommended", id = 223)
   void suggestCustomInterceptorInheritance(String customInterceptorClassName);

   @Message(value = "Custom interceptor '%s' specifies more than one position", id = 224)
   CacheConfigurationException multipleCustomInterceptorPositions(String customInterceptorClassName);

   @Message(value = "Custom interceptor '%s' doesn't specify a position", id = 225)
   CacheConfigurationException missingCustomInterceptorPosition(String customInterceptorClassName);

   @Message(value = "Error while initializing SSL context", id = 226)
   CacheConfigurationException sslInitializationException(@Cause Throwable e);

   @LogMessage(level = WARN)
   @Message(value = "Support for concurrent updates can no longer be configured (it is always enabled by default)", id = 227)
   void warnConcurrentUpdateSupportCannotBeConfigured();

   @LogMessage(level = ERROR)
   @Message(value = "Failed to recover cache %s state after the current node became the coordinator", id = 228)
   void failedToRecoverCacheState(String cacheName, @Cause Throwable cause);

   @Message(value = "Unexpected initial version type (only NumericVersion instances supported): %s", id = 229)
   IllegalArgumentException unexpectedInitialVersion(String className);

   @LogMessage(level = ERROR)
   @Message(value = "Failed to start rebalance for cache %s", id = 230)
   void rebalanceStartError(String cacheName, @Cause Throwable cause);

   @Message(value="Cache mode should be DIST or REPL, rather than %s", id = 231)
   IllegalStateException requireDistOrReplCache(String cacheType);

   @Message(value="Cache is in an invalid state: %s", id = 232)
   IllegalStateException invalidCacheState(String cacheState);

   @LogMessage(level = WARN)
   @Message(value = "Root element for %s already registered in ParserRegistry by %s. Cannot install %s.", id = 234)
   void parserRootElementAlreadyRegistered(QName qName, String oldParser, String newParser);

   @Message(value = "Configuration parser %s does not declare any Namespace annotations", id = 235)
   CacheConfigurationException parserDoesNotDeclareNamespaces(String name);

   @Message(value = "Purging expired entries failed", id = 236)
   PersistenceException purgingExpiredEntriesFailed(@Cause Throwable cause);

   @Message(value = "Waiting for expired entries to be purge timed out", id = 237)
   PersistenceException timedOutWaitingForExpiredEntriesToBePurged(@Cause Throwable cause);

   @Message(value = "Directory %s does not exist and cannot be created!", id = 238)
   CacheConfigurationException directoryCannotBeCreated(String path);

   @Message(value="Cache manager is shutting down, so type write externalizer for type=%s cannot be resolved", id = 239)
   IOException externalizerTableStopped(String className);

   @Message(value="Cache manager is shutting down, so type (id=%d) cannot be resolved. Interruption being pushed up.", id = 240)
   IOException pushReadInterruptionDueToCacheManagerShutdown(int readerIndex, @Cause InterruptedException cause);

   @Message(value="Cache manager is %s and type (id=%d) cannot be resolved (thread not interrupted)", id = 241)
   CacheException cannotResolveExternalizerReader(ComponentStatus status, int readerIndex);

   @Message(value="Missing foreign externalizer with id=%s, either externalizer was not configured by client, or module lifecycle implementation adding externalizer was not loaded properly", id = 242)
   CacheException missingForeignExternalizer(int foreignId);

   @Message(value="Type of data read is unknown. Id=%d is not amongst known reader indexes.", id = 243)
   CacheException unknownExternalizerReaderIndex(int readerIndex);

   @Message(value="AdvancedExternalizer's getTypeClasses for externalizer %s must return a non-empty set", id = 244)
   CacheConfigurationException advanceExternalizerTypeClassesUndefined(String className);

   @Message(value="Duplicate id found! AdvancedExternalizer id=%d for %s is shared by another externalizer (%s). Reader index is %d", id = 245)
   CacheConfigurationException duplicateExternalizerIdFound(int externalizerId, Class<?> typeClass, String otherExternalizer, int readerIndex);

   @Message(value="Internal %s externalizer is using an id(%d) that exceeded the limit. It needs to be smaller than %d", id = 246)
   CacheConfigurationException internalExternalizerIdLimitExceeded(AdvancedExternalizer<?> ext, int externalizerId, int maxId);

   @Message(value="Foreign %s externalizer is using a negative id(%d). Only positive id values are allowed.", id = 247)
   CacheConfigurationException foreignExternalizerUsingNegativeId(AdvancedExternalizer<?> ext, int externalizerId);

   @Message(value =  "Invalid cache loader configuration!!  Only ONE cache loader may have fetchPersistentState set " +
         "to true.  Cache will not start!", id = 248)
   CacheConfigurationException multipleCacheStoresWithFetchPersistentState();

   @Message(value =  "The cache loader configuration %s does not specify the loader class using @ConfigurationFor", id = 249)
   CacheConfigurationException loaderConfigurationDoesNotSpecifyLoaderClass(String className);

   @Message(value = "Invalid configuration, expecting '%s' got '%s' instead", id = 250)
   CacheConfigurationException incompatibleLoaderConfiguration(String expected, String actual);

   @Message(value = "Cache Loader configuration cannot be null", id = 251)
   CacheConfigurationException cacheLoaderConfigurationCannotBeNull();

   @LogMessage(level = ERROR)
   @Message(value = "Error executing parallel store task", id = 252)
   void errorExecutingParallelStoreTask(@Cause Throwable cause);

   @Message(value = "Invalid Cache Loader class: %s", id = 253)
   CacheConfigurationException invalidCacheLoaderClass(String name);

   @LogMessage(level = WARN)
   @Message(value = "The transport element's 'strictPeerToPeer' attribute is no longer in use.", id = 254)
   void strictPeerToPeerDeprecated();

   @LogMessage(level = ERROR)
   @Message(value = "Error while processing prepare", id = 255)
   void errorProcessingPrepare(@Cause Throwable e);

   @LogMessage(level = ERROR)
   @Message(value = "Configurator SAXParse error", id = 256)
   void configuratorSAXParseError(@Cause Exception e);

   @LogMessage(level = ERROR)
   @Message(value = "Configurator SAX error", id = 257)
   void configuratorSAXError(@Cause Exception e);

   @LogMessage(level = ERROR)
   @Message(value = "Configurator general error", id = 258)
   void configuratorError(@Cause Exception e);

   @LogMessage(level = ERROR)
   @Message(value = "Async store executor did not stop properly", id = 259)
   void errorAsyncStoreNotStopped();

   @LogMessage(level = ERROR)
   @Message(value = "Exception executing command", id = 260)
   void exceptionExecutingInboundCommand(@Cause Exception e);

   @LogMessage(level = ERROR)
   @Message(value = "Failed to execute outbound transfer", id = 261)
   void failedOutBoundTransferExecution(@Cause Throwable e);

   @LogMessage(level = ERROR)
   @Message(value = "Failed to enlist TransactionXaAdapter to transaction", id = 262)
   void failedToEnlistTransactionXaAdapter(@Cause Throwable e);

   @LogMessage(level = WARN)
   @Message(value = "FIFO strategy is deprecated, LRU will be used instead", id = 263)
   void warnFifoStrategyIsDeprecated();

   @LogMessage(level = WARN)
   @Message(value = "Not using an L1 invalidation reaper thread. This could lead to memory leaks as the requestors map may grow indefinitely!", id = 264)
   void warnL1NotHavingReaperThread();

   @LogMessage(level = WARN)
   @Message(value = "Problems creating interceptor %s", id = 267)
   void unableToCreateInterceptor(Class type, @Cause Exception e);

   @LogMessage(level = WARN)
   @Message(value = "Unable to broadcast evicts as a part of the prepare phase. Rolling back.", id = 268)
   void unableToRollbackEvictionsDuringPrepare(@Cause Throwable e);

   @LogMessage(level = WARN)
   @Message(value = "Cache used for Grid metadata should be synchronous.", id = 269)
   void warnGridFSMetadataCacheRequiresSync();

   @LogMessage(level = WARN)
   @Message(value = "Could not commit local tx %s", id = 270)
   void warnCouldNotCommitLocalTx(Object transactionDescription, @Cause Exception e);

   @LogMessage(level = WARN)
   @Message(value = "Could not rollback local tx %s", id = 271)
   void warnCouldNotRollbackLocalTx(Object transactionDescription, @Cause Exception e);

   @LogMessage(level = WARN)
   @Message(value = "Exception removing recovery information", id = 272)
   void warnExceptionRemovingRecovery(@Cause Exception e);

   @Message(value = "Indexing can not be enabled on caches in Invalidation mode", id = 273)
   CacheConfigurationException invalidConfigurationIndexingWithInvalidation();

   @LogMessage(level = ERROR)
   @Message(value = "Persistence enabled without any CacheLoaderInterceptor in InterceptorChain!", id = 274)
   void persistenceWithoutCacheLoaderInterceptor();

   @LogMessage(level = ERROR)
   @Message(value = "Persistence enabled without any CacheWriteInterceptor in InterceptorChain!", id = 275)
   void persistenceWithoutCacheWriteInterceptor();

   @Message(value = "Could not find migration data in cache %s", id = 276)
   CacheException missingMigrationData(String name);

   @LogMessage(level = WARN)
   @Message(value = "Could not migrate key %s", id = 277)
   void keyMigrationFailed(String key, @Cause Throwable cause);

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
   @Message(value = "Unable to send X-Site state chunk to '%s'.", id = 289)
   void unableToSendXSiteState(String site, @Cause Throwable cause);

   @LogMessage(level = WARN)
   @Message(value = "Unable to wait for X-Site state chunk ACKs from '%s'.", id = 290)
   void unableToWaitForXSiteStateAcks(String site, @Cause Throwable cause);

   @LogMessage(level = WARN)
   @Message(value = "Unable to apply X-Site state chunk.", id = 291)
   void unableToApplyXSiteState(@Cause Throwable cause);

   @LogMessage(level = WARN)
   @Message(value = "Unrecognized attribute '%s'. Please check your configuration. Ignoring!", id = 292)
   void unrecognizedAttribute(String property);

   @LogMessage(level = INFO)
   @Message(value = "Ignoring XML attribute %s, please remove from configuration file", id = 293)
   void ignoreXmlAttribute(Object attribute);

   @LogMessage(level = INFO)
   @Message(value = "Ignoring XML element %s, please remove from configuration file", id = 294)
   void ignoreXmlElement(Object element);

   @Message(value = "No thread pool with name %s found", id = 295)
   CacheConfigurationException undefinedThreadPoolName(String name);

   @Message(value = "Attempt to add a %s permission to a SecurityPermissionCollection", id = 296)
   IllegalArgumentException invalidPermission(Permission permission);

   @Message(value = "Attempt to add a permission to a read-only SecurityPermissionCollection", id = 297)
   SecurityException readOnlyPermissionCollection();

   @LogMessage(level = DEBUG)
   @Message(value = "Using internal security checker", id = 298)
   void authorizationEnabledWithoutSecurityManager();

   @Message(value = "Unable to acquire lock after %s for key %s and requestor %s. Lock is held by %s", id = 299)
   TimeoutException unableToAcquireLock(String timeout, Object key, Object requestor, Object owner);

   @Message(value = "There was an exception while processing retrieval of entry values", id = 300)
   CacheException exceptionProcessingEntryRetrievalValues(@Cause Throwable cause);

   @Message(value = "Iterator response for identifier %s encountered unexpected exception", id = 301)
   CacheException exceptionProcessingIteratorResponse(UUID identifier, @Cause Throwable cause);

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

   @LogMessage(level = INFO)
   @Message(value = "Starting cluster-wide rebalance for cache %s, topology %s", id = 310)
   void startRebalance(String cacheName, CacheTopology cacheTopology);

   @LogMessage(level = DEBUG)
   @Message(value = "Received a command from an outdated topology, returning the exception to caller", id = 311)
   void outdatedTopology(@Cause Throwable oe);

   // Messages between 312 and 320 have been moved to the org.infinispan.util.logging.events.Messages class

   @LogMessage(level = WARN)
   @Message(value = "Cyclic dependency detected between caches, stop order ignored", id = 321)
   void stopOrderIgnored();

   @LogMessage(level = WARN)
   @Message(value = "Unable to re-start x-site state transfer to site %s", id = 322)
   void failedToRestartXSiteStateTransfer(String siteName, @Cause Throwable cause);

   @Message(value = "%s is in 'TERMINATED' state and so it does not accept new invocations. " +
         "Either restart it or recreate the cache container.", id = 323)
   IllegalLifecycleStateException cacheIsTerminated(String cacheName);

   @Message(value = "%s is in 'STOPPING' state and this is an invocation not belonging to an on-going transaction, so it does not accept new invocations. " +
         "Either restart it or recreate the cache container.", id = 324)
   IllegalLifecycleStateException cacheIsStopping(String cacheName);

   @Message (value="Creating tmp cache %s timed out waiting for rebalancing to complete on node %s ", id=325)
   RuntimeException creatingTmpCacheTimedOut(String cacheName, Address address);

   @LogMessage(level = WARN)
   @Message(value = "Remote transaction %s timed out. Rolling back after %d ms", id = 326)
   void remoteTransactionTimeout(GlobalTransaction gtx, long ageMilliSeconds);

   @Message(value = "Cannot find a parser for element '%s' in namespace '%s'. Check that your configuration is up-to date for this version of Infinispan.", id = 327)
   CacheConfigurationException unsupportedConfiguration(String element, String namespaceUri);

   @LogMessage(level = DEBUG)
   @Message(value = "Finished local rebalance for cache %s on node %s, topology id = %d", id = 328)
   void rebalanceCompleted(String cacheName, Address node, int topologyId);

   @LogMessage(level = WARN)
   @Message(value = "Unable to read rebalancing status from coordinator %s", id = 329)
   void errorReadingRebalancingStatus(Address coordinator, @Cause Exception e);

   @LogMessage(level = WARN)
   @Message(value = "Distributed task failed at %s. The task is failing over to be executed at %s", id = 330)
   void distributedTaskFailover(Address failedAtAddress, Address failoverTarget, @Cause Exception e);

   @LogMessage(level = WARN)
   @Message(value = "Unable to invoke method %s on Object instance %s ", id = 331)
   void unableToInvokeListenerMethod(Method m, Object target, @Cause Throwable e);

   @Message(value = "Remote transaction %s rolled back because originator is no longer in the cluster", id = 332)
   CacheException orphanTransactionRolledBack(GlobalTransaction gtx);

   @Message(value = "The site must be specified.", id = 333)
   CacheConfigurationException backupSiteNullName();

   @Message(value = "Using a custom failure policy requires a failure policy class to be specified.", id = 334)
   CacheConfigurationException customBackupFailurePolicyClassNotSpecified();

   @Message(value = "Two-phase commit can only be used with synchronous backup strategy.", id = 335)
   CacheConfigurationException twoPhaseCommitAsyncBackup();

   @LogMessage(level = INFO)
   @Message(value = "Finished cluster-wide rebalance for cache %s, topology id = %d", id = 336)
   void clusterWideRebalanceCompleted(String cacheName, int topologyId);

   @Message(value = "The 'site' must be specified!", id = 337)
   CacheConfigurationException backupMissingSite();

   @Message(value = "It is required to specify a 'failurePolicyClass' when using a custom backup failure policy!", id = 338)
   CacheConfigurationException missingBackupFailurePolicyClass();

   @Message(value = "Null name not allowed (use 'defaultRemoteCache()' in case you want to specify the default cache name).", id = 339)
   CacheConfigurationException backupForNullCache();

   @Message(value = "Both 'remoteCache' and 'remoteSite' must be specified for a backup'!", id = 340)
   CacheConfigurationException backupForMissingParameters();

   @Message(value = "Cannot configure async properties for an sync cache. Set the cache mode to async first.", id = 341)
   IllegalStateException asyncPropertiesConfigOnSyncCache();

   @Message(value = "Cannot configure sync properties for an async cache. Set the cache mode to sync first.", id = 342)
   IllegalStateException syncPropertiesConfigOnAsyncCache();

   @Message(value = "Must have a transport set in the global configuration in " +
               "order to define a clustered cache", id = 343)
   CacheConfigurationException missingTransportConfiguration();

   @Message(value = "reaperWakeUpInterval must be >= 0, we got %d", id = 344)
   CacheConfigurationException invalidReaperWakeUpInterval(long timeout);

   @Message(value = "completedTxTimeout must be >= 0, we got %d", id = 345)
   CacheConfigurationException invalidCompletedTxTimeout(long timeout);

   @Message(value = "Total Order based protocol not available for transaction mode %s", id = 346)
   CacheConfigurationException invalidTxModeForTotalOrder(TransactionMode transactionMode);

   @Message(value = "Cache mode %s is not supported by Total Order based protocol", id = 347)
   CacheConfigurationException invalidCacheModeForTotalOrder(String friendlyCacheModeString);

   @Message(value = "Total Order based protocol not available with recovery", id = 348)
   CacheConfigurationException unavailableTotalOrderWithTxRecovery();

   @Message(value = "Total Order based protocol not available with %s", id = 349)
   CacheConfigurationException invalidLockingModeForTotalOrder(LockingMode lockingMode);

   @Message(value = "Enabling the L1 cache is only supported when using DISTRIBUTED as a cache mode.  Your cache mode is set to %s", id = 350)
   CacheConfigurationException l1OnlyForDistributedCache(String cacheMode);

   @Message(value = "Using a L1 lifespan of 0 or a negative value is meaningless", id = 351)
   CacheConfigurationException l1InvalidLifespan();

   @Message(value = "Cannot define both interceptor class (%s) and interceptor instance (%s)", id = 354)
   CacheConfigurationException interceptorClassAndInstanceDefined(String customInterceptorClassName, String customInterceptor);

   @Message(value = "Unable to instantiate loader/writer instance for StoreConfiguration %s", id = 355)
   CacheConfigurationException unableToInstantiateClass(Class<?> storeConfigurationClass);

   @Message(value = "Maximum data container size is currently 2^48 - 1, the number provided was %s", id = 356)
   CacheConfigurationException evictionSizeTooLarge(long value);

   @LogMessage(level = ERROR)
   @Message(value = "end() failed for %s", id = 357)
   void xaResourceEndFailed(XAResource resource, @Cause Throwable t);

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
   @Message(value = "No filter indexing service provider found for filter of type %s", id = 363)
   void noFilterIndexingServiceProviderFound(String filterClassName);

   @Message(value = "Attempted to register cluster listener of class %s, but listener is annotated as only observing pre events!", id = 364)
   CacheException clusterListenerRegisteredWithOnlyPreEvents(Class<?> listenerClass);

   @Message(value = "Could not find the specified JGroups configuration file '%s'", id = 365)
   CacheConfigurationException jgroupsConfigurationNotFound(String cfg);

   @Message(value = "Unable to add a 'null' Custom Cache Store", id = 366)
   IllegalArgumentException unableToAddNullCustomStore();

   @LogMessage(level = ERROR)
   @Message(value = "There was an issue with topology update for topology: %s", id = 367)
   void topologyUpdateError(int topologyId, @Cause Throwable t);

   @LogMessage(level = WARN)
   @Message(value = "Memory approximation calculation for eviction is unsupported for the '%s' Java VM", id = 368)
   void memoryApproximationUnsupportedVM(String javaVM);

   @LogMessage(level = WARN)
   @Message(value = "Ignoring asyncMarshalling configuration", id = 369)
   void ignoreAsyncMarshalling();

   @Message(value = "Cache name '%s' cannot be used as it is a reserved, internal name", id = 370)
   IllegalArgumentException illegalCacheName(String name);

   @Message(value = "Cannot remove cache configuration '%s' because it is in use", id = 371)
   IllegalStateException configurationInUse(String configurationName);

   @Message(value = "Statistics are enabled while attribute 'available' is set to false.", id = 372)
   CacheConfigurationException statisticsEnabledNotAvailable();

   @Message(value = "Attempted to start a cache using configuration template '%s'", id = 373)
   CacheConfigurationException templateConfigurationStartAttempt(String cacheName);

   @Message(value = "No such template '%s' when declaring '%s'", id = 374)
   CacheConfigurationException undeclaredConfiguration(String extend, String name);

   @Message(value = "Cannot use configuration '%s' as a template", id = 375)
   CacheConfigurationException noConfiguration(String extend);

   @Message(value = "Interceptor stack is not supported in simple cache", id = 376)
   UnsupportedOperationException interceptorStackNotSupported();

   @Message(value = "Explicit lock operations are not supported in simple cache", id = 377)
   UnsupportedOperationException lockOperationsNotSupported();

   @Message(value = "Invocation batching not enabled in current configuration! Please enable it.", id = 378)
   CacheConfigurationException invocationBatchingNotEnabled();

   @Message(value = "Distributed Executors Framework is not supported in simple cache", id = 380)
   CacheConfigurationException distributedExecutorsNotSupported();

   @Message(value = "This configuration is not supported for simple cache", id = 381)
   CacheConfigurationException notSupportedInSimpleCache();

   @LogMessage(level = WARN)
   @Message(value = "Global state persistence was enabled without specifying a location", id = 382)
   void missingGlobalStatePersistentLocation();

   @LogMessage(level = WARN)
   @Message(value = "The eviction max-entries attribute has been deprecated. Please use the size attribute instead", id = 383)
   void evictionMaxEntriesDeprecated();

   @Message(value = "Unable to broadcast invalidation messages", id = 384)
   RuntimeException unableToBroadcastInvalidation(@Cause Throwable e);

   @LogMessage(level = WARN)
   @Message(value = "The data container class configuration has been deprecated.  This has no current replacement", id = 385)
   void dataContainerConfigurationDeprecated();

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

   @Message(value = "Recovery not supported with Asynchronous %s cache mode", id = 393)
   CacheConfigurationException recoveryNotSupportedWithAsync(String cacheMode);

   @Message(value = "Recovery not supported with asynchronous commit phase", id = 394)
   CacheConfigurationException recoveryNotSupportedWithAsyncCommit();

   @LogMessage(level = INFO)
   @Message(value = "Transaction notifications are disabled.  This prevents cluster listeners from working properly!", id = 395)
   void transactionNotificationsDisabled();

   @LogMessage(level = WARN)
   @Message(value = "Received unsolicited state from node %s for segment %d of cache %s", id = 396)
   void ignoringUnsolicitedState(Address node, int segment, String cacheName);

   @Message(value = "Could not migrate data for cache %s, check remote store config in the target cluster", id = 397)
   CacheException couldNotMigrateData(String name);

   @Message(value ="CH Factory '%s' cannot restore a persisted CH of class '%s'", id = 398)
   IllegalStateException persistentConsistentHashMismatch(String hashFactory, String consistentHashClass);

   @Message(value = "Timeout while waiting for %d members in cluster. Last view had %s", id = 399)
   TimeoutException timeoutWaitingForInitialNodes(int initialClusterSize, List<?> members);

   @Message(value = "Node %s was suspected", id = 400)
   SuspectException remoteNodeSuspected(Address address);

   @Message(value = "Node %s timed out, time : %s %s", id = 401)
   TimeoutException remoteNodeTimedOut(Address address, long time, TimeUnit unit);

   @Message(value = "Timeout waiting for view %d. Current view is %d, current status is %s", id = 402)
   TimeoutException timeoutWaitingForView(int expectedViewId, int currentViewId,
         Object clusterManagerStatus);

   @LogMessage(level = WARN)
   @Message(value = "No indexable classes were defined for this indexed cache; switching to autodetection (support for autodetection will be removed in Infinispan 9.0).", id = 403)
   void noIndexableClassesDefined();

   @Message(value = "The configured entity class %s is not indexable. Please remove it from the indexing configuration.", id = 404)
   CacheConfigurationException classNotIndexable(String className);

   @LogMessage(level = ERROR)
   @Message(value = "Caught exception while invoking a cache manager listener!", id = 405)
   void failedInvokingCacheManagerListener(@Cause Exception e);

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

   @LogMessage(level = WARN)
   @Message(value = "Classpath does not look correct. Make sure you are not mixing uber and jars", id = 411)
   void warnAboutUberJarDuplicates();

   @Message(value = "Cannot determine a synthetic transaction configuration from mode=%s, xaEnabled=%s, recoveryEnabled=%s, batchingEnabled=%s", id = 412)
   CacheConfigurationException unknownTransactionConfiguration(org.infinispan.transaction.TransactionMode mode, boolean xaEnabled, boolean recoveryEnabled, boolean batchingEnabled);

   @Message(value = "Unable to instantiate serializer for StoreConfiguration %s", id = 413)
   CacheConfigurationException unableToInstantiateSerializer(Class<?> storeConfigurationClass);

   @Message(value = "Global security authorization should be enabled if cache authorization enabled.", id = 414)
   CacheConfigurationException globalSecurityAuthShouldBeEnabled();

   @LogMessage(level = WARN)
   @Message(value = "The %s is no longer supported since version %s. Attribute %s on line %d will be ignored.", id = 415)
   void ignoredAttribute(String componentName, String version, String attributeName, int line);

   @LogMessage(level = ERROR)
   @Message(value = "Error executing submitted store task", id = 416)
   void errorExecutingSubmittedStoreTask(@Cause Throwable cause);

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

   @Message(value="Duplicate id found! AdvancedExternalizer id=%d is shared by another externalizer (%s)", id = 423)
   CacheConfigurationException duplicateExternalizerIdFound(int externalizerId, String otherExternalizer);

   @Message(value = "Eviction size value cannot be less than or equal to zero if eviction is enabled", id = 424)
   CacheConfigurationException invalidEvictionSize();

   @Message(value = "Eviction cannot use memory-based approximation with LIRS", id = 425)
   CacheConfigurationException memoryEvictionInvalidStrategyLIRS();

   @Message(value = "Timeout after %s waiting for acks. Missing acks are %s", id = 426)
   TimeoutException timeoutWaitingForAcks(String timeout, String missingAcks);

   @Message(value = "Timeout after %s waiting for acks", id = 427)
   TimeoutException timeoutWaitingForAcks(String timeout);
}

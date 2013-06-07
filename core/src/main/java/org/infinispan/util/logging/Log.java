/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan.util.logging;

import org.infinispan.CacheException;
import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.config.ConfigurationException;
import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.loaders.bucket.Bucket;
import org.infinispan.loaders.decorators.SingletonStore;
import org.infinispan.remoting.RemoteException;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.transport.Address;
import org.infinispan.transaction.LocalTransaction;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.transaction.xa.recovery.RecoveryAwareRemoteTransaction;
import org.infinispan.transaction.xa.recovery.RecoveryAwareTransaction;
import org.infinispan.util.TypedProperties;
import org.jboss.logging.BasicLogger;
import org.jboss.logging.Cause;
import org.jboss.logging.LogMessage;
import org.jboss.logging.Message;
import org.jboss.logging.MessageLogger;
import org.jgroups.View;

import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanRegistrationException;
import javax.management.ObjectName;
import javax.naming.NamingException;
import javax.transaction.Synchronization;
import javax.transaction.TransactionManager;
import javax.transaction.xa.XAException;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URL;
import java.nio.channels.FileChannel;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.jboss.logging.Logger.Level.*;

/**
 * Infinispan's log abstraction layer on top of JBoss Logging.
 * <p/>
 * It contains explicit methods for all INFO or above levels so that they can
 * be internationalized. For the core module, message ids ranging from 0001
 * to 1000 inclusively have been reserved.
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
 */
@MessageLogger(projectCode = "ISPN")
public interface Log extends BasicLogger {

   @LogMessage(level = WARN)
   @Message(value = "Unable to load %s from cache loader", id = 1)
   void unableToLoadFromCacheLoader(Object key, @Cause CacheLoaderException cle);

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

   @LogMessage(level = INFO)
   @Message(value = "MBeans were successfully registered to the platform MBean server.", id = 31)
   void mbeansSuccessfullyRegistered();

   @LogMessage(level = WARN)
   @Message(value = "Problems un-registering MBeans", id = 32)
   void problemsUnregisteringMBeans(@Cause Exception e);

   @LogMessage(level = WARN)
   @Message(value = "Unable to unregister Cache MBeans with pattern %s", id = 33)
   void unableToUnregisterMBeanWithPattern(String pattern, @Cause MBeanRegistrationException e);

   @LogMessage(level = ERROR)
   @Message(value = "There's already an cache manager instance registered under " +
         "'%s' JMX domain. If you want to allow multiple instances configured " +
         "with same JMX domain enable 'allowDuplicateDomains' attribute in " +
         "'globalJmxStatistics' config element", id = 34)
   void cacheManagerAlreadyRegistered(String jmxDomain);

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
   @Message(value = "Interrupted or timeout while waiting for AsyncStore worker threads to push all state to the decorated store", id = 48)
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
   @Message(value = "AsyncStoreCoordinator interrupted", id = 54)
   void asyncStoreCoordinatorInterrupted(@Cause InterruptedException e);

   @LogMessage(level = ERROR)
   @Message(value = "Unexpected error in AsyncStoreCoordinator thread. AsyncStore is dead!", id = 55)
   void unexpectedErrorInAsyncStoreCoordinator(@Cause Throwable t);

   @LogMessage(level = ERROR)
   @Message(value = "Error while handling Modification in AsyncStore", id = 56)
   void errorModifyingAsyncStore(@Cause Exception e);

   @LogMessage(level = ERROR)
   @Message(value = "Clear() operation in async store could not be performed", id = 57)
   void unableToClearAsyncStore();

   @LogMessage(level = ERROR)
   @Message(value = "Exception reported changing cache active status", id = 58)
   void errorChangingSingletonStoreStatus(@Cause SingletonStore.PushStateException e);

   @LogMessage(level = WARN)
   @Message(value = "Had problems removing file %s", id = 59)
   void problemsRemovingFile(File f);

   @LogMessage(level = WARN)
   @Message(value = "Problems purging file %s", id = 60)
   void problemsPurgingFile(File buckedFile, @Cause CacheLoaderException e);

   @LogMessage(level = WARN)
   @Message(value = "Unable to acquire global lock to purge cache store", id = 61)
   void unableToAcquireLockToPurgeStore();

   @LogMessage(level = ERROR)
   @Message(value = "Error while reading from file: %s", id = 62)
   void errorReadingFromFile(File f, @Cause Exception e);

   @LogMessage(level = ERROR)
   @Message(value = "Exception while saving bucket %s", id = 63)
   void errorSavingBucket(Bucket b, @Cause IOException ex);

   @LogMessage(level = WARN)
   @Message(value = "Problems creating the directory: %s", id = 64)
   void problemsCreatingDirectory(File dir);

   @LogMessage(level = ERROR)
   @Message(value = "Exception while marshalling object", id = 65)
   void errorMarshallingObject(@Cause IOException ioe);

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
   @Message(value = "Failed replicating %d elements in replication queue", id = 72)
   void failedReplicatingQueue(int size, @Cause Throwable t);

   @LogMessage(level = ERROR)
   @Message(value = "Unexpected error while replicating", id = 73)
   void unexpectedErrorReplicating(@Cause Throwable t);

   @LogMessage(level = ERROR)
   @Message(value = "Message or message buffer is null or empty.", id = 77)
   void msgOrMsgBufferEmpty();

   @LogMessage(level = INFO)
   @Message(value = "Starting JGroups Channel", id = 78)
   void startingJGroupsChannel();

   @LogMessage(level = INFO)
   @Message(value = "Cache local address is %s, physical addresses are %s", id = 79)
   void localAndPhysicalAddress(Address address, List<Address> physicalAddresses);

   @LogMessage(level = INFO)
   @Message(value = "Disconnecting and closing JGroups Channel", id = 80)
   void disconnectAndCloseJGroups();

   @LogMessage(level = ERROR)
   @Message(value = "Problem closing channel; setting it to null", id = 81)
   void problemClosingChannel(@Cause Exception e);

   @LogMessage(level = INFO)
   @Message(value = "Stopping the RpcDispatcher", id = 82)
   void stoppingRpcDispatcher();

   @LogMessage(level = ERROR)
   @Message(value = "Class [%s] cannot be cast to JGroupsChannelLookup! Not using a channel lookup.", id = 83)
   void wrongTypeForJGroupsChannelLookup(String channelLookupClassName, @Cause Exception e);

   @LogMessage(level = ERROR)
   @Message(value = "Errors instantiating [%s]! Not using a channel lookup.", id = 84)
   void errorInstantiatingJGroupsChannelLookup(String channelLookupClassName, @Cause Exception e);

   @LogMessage(level = ERROR)
   @Message(value = "Error while trying to create a channel using config files: %s", id = 85)
   void errorCreatingChannelFromConfigFile(String cfg);

   @LogMessage(level = ERROR)
   @Message(value = "Error while trying to create a channel using config XML: %s", id = 86)
   void errorCreatingChannelFromXML(String cfg);

   @LogMessage(level = ERROR)
   @Message(value = "Error while trying to create a channel using config string: %s", id = 87)
   void errorCreatingChannelFromConfigString(String cfg);

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
   @Message(value = "Received new, MERGED cluster view: %s", id = 93)
   void receivedMergedView(View newView);

   @LogMessage(level = INFO)
   @Message(value = "Received new cluster view: %s", id = 94)
   void receivedClusterView(View newView);

   @LogMessage(level = ERROR)
   @Message(value = "Error while processing a prepare in a single-phase transaction", id = 97)
   void errorProcessing1pcPrepareCommand(@Cause Throwable e);

   @LogMessage(level = ERROR)
   @Message(value = "Exception while rollback", id = 98)
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
   void unableToInvokeListenerMethod(Method m, Object target, @Cause Throwable e);

   @LogMessage(level = WARN)
   @Message(value = "Could not lock key %s in order to invalidate from L1 at node %s, skipping....", id = 135)
   void unableToLockToInvalidate(Object key, Address address);

   @LogMessage(level = ERROR)
   @Message(value = "Execution error", id = 136)
   void executionError(@Cause Throwable t);

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
   @Message(value = "The async store shutdown timeout (%d ms) is too high compared " +
         "to cache stop timeout (%d ms), so instead using %d ms for async store stop wait", id = 142)
   void asyncStoreShutdownTimeoutTooHigh(long configuredAsyncStopTimeout,
      long cacheStopTimeout, long asyncStopTimeout);

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
   @Message(value = "Failed to recover cluster state after the current node became the coordinator", id = 196)
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
   @Message(value = "Could not execute cancelation command locally %s", id=206)
   void couldNotExecuteCancellationLocally(String message);

   @LogMessage(level = WARN)
   @Message(value = "Could not interrupt as no thread found for command uuid %s", id=207)
   void couldNotInterruptThread(UUID id);

   @LogMessage(level = ERROR)
   @Message(value = "No live owners found for segment %d of cache %s. Current owners are:  %s. Faulty owners: %s", id=208)
   void noLiveOwnersFoundForSegment(int segmentId, String cacheName, Collection<Address> owners, Collection<Address> faultySources);

   @LogMessage(level = WARN)
   @Message(value = "Failed to retrieve transactions for segments %s of cache %s from node %s (node will not be retried)", id=209)
   void failedToRetrieveTransactionsForSegments(Collection<Integer> segments, String cacheName, Address source, @Cause Exception e);

   @LogMessage(level = WARN)
   @Message(value = "Failed to request segments %s of cache %s from node %s (node will not be retried)", id=210)
   void failedToRequestSegments(Collection<Integer> segments, String cacheName, Address source, @Cause Exception e);

   @LogMessage(level = WARN)
   @Message(value = "Transactions were requested by node %s with topology %d, older than the local topology (%d)", id=211)
   void transactionsRequestedByNodeWithOlderTopology(Address node, int requestTopologyId, int localTopologyId);

   @LogMessage(level = WARN)
   @Message(value = "Segments were requested by node %s with topology %d, older than the local topology (%d)", id=212)
   void segmentsRequestedByNodeWithOlderTopology(Address node, int requestTopologyId, int localTopologyId);

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
   RemoteException remoteException(Address sender, @Cause Exception e);

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
   ConfigurationException customInterceptorMissingClass();

   @LogMessage(level = WARN)
   @Message(value = "Custom interceptor '%s' does not extend BaseCustomInterceptor, which is recommended", id = 223)
   void suggestCustomInterceptorInheritance(String customInterceptorClassName);

   @Message(value = "Custom interceptor '%s' specifies more than one position", id = 224)
   ConfigurationException multipleCustomInterceptorPositions(String customInterceptorClassName);

   @Message(value = "Custom interceptor '%s' doesn't specify a position", id = 225)
   ConfigurationException missingCustomInterceptorPosition(String customInterceptorClassName);

   @Message(value = "Error while initializing SSL context", id = 226)
   ConfigurationException sslInitializationException(@Cause Throwable e);

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
}


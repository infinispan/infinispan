package org.infinispan.cli.interpreter.logging;

import static org.jboss.logging.Logger.Level.ERROR;
import static org.jboss.logging.Logger.Level.WARN;

import java.util.ServiceConfigurationError;

import javax.transaction.SystemException;

import org.infinispan.cli.interpreter.codec.CodecException;
import org.infinispan.cli.interpreter.result.StatementException;
import org.infinispan.commons.CacheException;
import org.jboss.logging.annotations.Cause;
import org.jboss.logging.annotations.LogMessage;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageLogger;

/**
 * Log abstraction for the ISPNCLIQL interpreter. For this module, message ids
 * ranging from 19001 to 20000 inclusively have been reserved.
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
@MessageLogger(projectCode = "ISPN")
public interface Log extends org.infinispan.util.logging.Log {
   @LogMessage(level = ERROR)
   @Message(value = "Could not register interpreter MBean", id = 19001)
   void jmxRegistrationFailed();

   @LogMessage(level = ERROR)
   @Message(value = "Could not unregister interpreter MBean", id = 19002)
   void jmxUnregistrationFailed();

   @LogMessage(level = ERROR)
   @Message(value = "Interpreter error", id = 19003)
   void interpreterError(@Cause Throwable t);

   @Message(value = "No action has been specified for the upgrade command", id = 19004)
   StatementException missingUpgradeAction();

   @Message(value = "No migrator has been specified", id = 19005)
   StatementException missingMigrator();

   @Message(value = "Unknown option '%s'", id = 19006)
   StatementException unknownOption(String name);

   @Message(value = "The option %s requires a parameter", id = 19007)
   StatementException missingOptionParameter(String name);

   @Message(value = "Failure while encoding key using codec '%s'", id = 19008)
   CodecException keyEncodingFailed(@Cause Exception e, String codec);

   @Message(value = "Failure while decoding key using codec '%s'", id = 19009)
   CodecException keyDecodingFailed(@Cause Exception e, String codec);

   @Message(value = "Failure while encoding value using codec '%s'", id = 19010)
   CodecException valueEncodingFailed(@Cause Exception e, String codec);

   @Message(value = "Failure while decoding value using codec '%s'", id = 19011)
   CodecException valueDecodingFailed(@Cause Exception e, String codec);

   @Message(value = "No such codec named '%s'", id = 19012)
   CodecException noSuchCodec(String codec);

   @LogMessage(level = WARN)
   @Message(value = "There was an error loading a codec", id = 19013)
   void loadingCodecFailed(@Cause ServiceConfigurationError e);

   @LogMessage(level = ERROR)
   @Message(value = "Codec '%s' attempts to override codec '%s'", id = 19014)
   void duplicateCodec(String codec1, String codec2);

   @Message(value = "Invalid session id '%s'", id = 19015)
   IllegalArgumentException invalidSession(String sessionId);

   @Message(value = "No such cache: '%s'", id = 19016)
   IllegalArgumentException nonExistentCache(String cacheName);

   @Message(value = "A cache named '%s' already exists", id = 19017)
   IllegalArgumentException cacheAlreadyExists(String cacheName);

   @Message(value = "Could not create cache named '%s' on all cluster nodes", id = 19018)
   CacheException cannotCreateClusteredCaches(@Cause Throwable e, String cacheName);

   @Message(value = "Statistics not enabled on cache '%s'", id = 19019)
   StatementException statisticsNotEnabled(String cacheName);

   @Message(value = "Cannot retrieve a transaction manager", id = 19020)
   StatementException noTransactionManager();

   @Message(value ="The TransactionManager does not support nested transactions", id = 19021)
   StatementException noNestedTransactions();

   @Message(value = "Unexpected error while starting transaction", id = 19022)
   StatementException unexpectedTransactionError(@Cause SystemException e);

   @Message(value = "Cannot commit transaction", id = 19023)
   StatementException cannotCommitTransaction(@Cause Exception e);

   @Message(value = "Cache is not distributed", id = 19024)
   StatementException cacheNotDistributed();

   @Message(value = "Cannot rollback transaction", id = 19025)
   StatementException cannotRollbackTransaction(@Cause Exception e);

   @Message(value = "An error occurred while synchronizing data for cache '%s' using migrator '%s' from the source server", id = 19026)
   StatementException dataSynchronizationError(@Cause Exception e, String migratorName, String name);

   @Message(value = "An error occurred while disconnecting the source server for cache '%s' using migrator '%s'", id = 19027)
   StatementException sourceDisconnectionError(@Cause Exception e, String migratorName, String name);

   @Message(value = "A site name needs to be specified", id = 19028)
   StatementException siteNameNotSpecified();

   @Message(value = "No cache selected yet", id = 19029)
   StatementException noCacheSelectedYet();

   @Message(value = "Failure while encoding value of type '%s' using codec '%s'", id = 19030)
   CodecException valueEncodingFailed(String type, String codec);

   @Message(value = "The container does not have authorization enabled", id = 19031)
   StatementException authorizationNotEnabledOnContainer();

   @Message(value = "The %s statement requires the ClusterPrincipalMapper", id = 19032)
   StatementException noClusterPrincipalMapper(String stmt);
}

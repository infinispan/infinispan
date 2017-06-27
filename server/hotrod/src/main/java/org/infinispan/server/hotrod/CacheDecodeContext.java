package org.infinispan.server.hotrod;

import static java.lang.String.format;

import java.util.concurrent.TimeUnit;

import javax.security.auth.Subject;
import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.RollbackException;
import javax.transaction.Status;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.tx.XidImpl;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.container.versioning.NumericVersion;
import org.infinispan.container.versioning.NumericVersionGenerator;
import org.infinispan.container.versioning.SimpleClusteredVersion;
import org.infinispan.container.versioning.VersionGenerator;
import org.infinispan.context.Flag;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.metadata.EmbeddedMetadata;
import org.infinispan.metadata.Metadata;
import org.infinispan.registry.InternalCacheRegistry;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.server.core.ServerConstants;
import org.infinispan.server.hotrod.logging.Log;
import org.infinispan.server.hotrod.tx.CommitTransactionDecodeContext;
import org.infinispan.server.hotrod.tx.PrepareTransactionDecodeContext;
import org.infinispan.server.hotrod.tx.RollbackTransactionDecodeContext;
import org.infinispan.server.hotrod.tx.SecondPhaseTransactionDecodeContext;
import org.infinispan.server.hotrod.tx.TxState;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.TransactionProtocol;
import org.infinispan.transaction.tm.EmbeddedTransactionManager;
import org.infinispan.util.concurrent.IsolationLevel;
import org.infinispan.util.logging.LogFactory;

/**
 * Invokes operations against the cache based on the state kept during decoding process
 */
public final class CacheDecodeContext {
   static final long MillisecondsIn30days = TimeUnit.DAYS.toMillis(30);
   static final Log log = LogFactory.getLog(CacheDecodeContext.class, Log.class);
   static final boolean isTrace = log.isTraceEnabled();

   private final HotRodServer server;

   CacheDecodeContext(HotRodServer server) {
      this.server = server;
   }

   VersionedDecoder decoder;
   HotRodHeader header;
   AdvancedCache<byte[], byte[]> cache;
   byte[] key;
   RequestParameters params;
   Object operationDecodeContext;
   Subject subject;

   public HotRodHeader getHeader() {
      return header;
   }

   public byte[] getKey() {
      return key;
   }

   public RequestParameters getParams() {
      return params;
   }

   /**
    * Handles a rollback request from a client.
    */
   TransactionResponse rollbackTransaction() {
      validateConfiguration();
      return finishTransaction(new RollbackTransactionDecodeContext(cache, (XidImpl) operationDecodeContext));
   }

   /**
    * Handles a prepare request from a client
    */
   Response prepareTransaction() {
      validateConfiguration();

      PrepareTransactionContext context = (PrepareTransactionContext) operationDecodeContext;
      if (context.isEmpty()) {
         //the client can optimize and avoid contacting the server when no data is written.
         if (isTrace) {
            log.tracef("Transaction %s is read only.", context.getXid());
         }
         return createTransactionResponse(header, XAResource.XA_RDONLY);
      }
      PrepareTransactionDecodeContext txContext = new PrepareTransactionDecodeContext(cache, context.getXid());

      Response response = checkExistingTxForPrepare(txContext);
      if (response != null) {
         if (isTrace) {
            log.tracef("Transaction %s conflicts with another node. Response is %s", context.getXid(), response);
         }
         return response;
      }

      if (!txContext.startTransaction()) {
         if (isTrace) {
            log.tracef("Unable to start transaction %s", context.getXid());
         }
         return decoder.createNotExecutedResponse(header, null);
      }

      //forces the write-lock. used by pessimistic transaction. it ensures the key is not written after is it read and validated
      //optimistic transaction will use the write-skew check.
      AdvancedCache<byte[], byte[]> txCache = txContext.decorateCache(cache);

      try {
         for (TransactionWrite write : context.writes()) {
            if (isValid(write, txCache)) {
               if (write.isRemove()) {
                  txCache.remove(write.key);
               } else {
                  txCache.put(write.key, write.value, buildMetadata(write.lifespan, write.maxIdle));
               }
            } else {
               txContext.setRollbackOnly();
               break;
            }
         }
         int xaCode = txContext.prepare(context.isOnePhaseCommit());
         return createTransactionResponse(header, xaCode);
      } catch (Exception e) {
         return createTransactionResponse(header, txContext.rollback());
      } finally {
         EmbeddedTransactionManager.dissociateTransaction();
      }
   }

   /**
    * Handles a commit request from a client
    */
   TransactionResponse commitTransaction() {
      validateConfiguration();
      return finishTransaction(new CommitTransactionDecodeContext(cache, (XidImpl) operationDecodeContext));
   }

   /**
    * Commits or Rollbacks the transaction (second phase of two-phase-commit)
    */
   private TransactionResponse finishTransaction(SecondPhaseTransactionDecodeContext txContext) {
      try {
         txContext.perform();
      } catch (HeuristicMixedException e) {
         return createTransactionResponse(header, XAException.XA_HEURMIX);
      } catch (HeuristicRollbackException e) {
         return createTransactionResponse(header, XAException.XA_HEURRB);
      } catch (RollbackException e) {
         return createTransactionResponse(header, XAException.XA_RBROLLBACK);
      }
      return createTransactionResponse(header, XAResource.XA_OK);
   }

   /**
    * Checks if the configuration (and the transaction manager) is able to handle client transactions.
    */
   private void validateConfiguration() {
      Configuration configuration = cache.getCacheConfiguration();
      if (!configuration.transaction().transactionMode().isTransactional()) {
         throw log.expectedTransactionalCache(cache.getName());
      }
      if (configuration.locking().isolationLevel() != IsolationLevel.REPEATABLE_READ) {
         throw log.unexpectedIsolationLevel(cache.getName());
      }

      //TODO because of ISPN-7672, optimistic and total order transactions needs versions. however, versioning is currently broken
      if (configuration.transaction().lockingMode() == LockingMode.OPTIMISTIC ||
            configuration.transaction().transactionProtocol() == TransactionProtocol.TOTAL_ORDER) {
         //no Log. see TODO.
         throw new IllegalStateException(
               format("Cache '%s' cannot use Optimistic neither Total Order transactions.", cache.getName()));
      }
   }

   /**
    * Checks if the transaction was already prepared in another node
    * <p>
    * The client can send multiple requests to the server (in case of timeout or similar). This request is ignored when
    * (1) the originator is still alive; (2) the transaction is prepared or committed/rolled-back
    * <p>
    * If the transaction isn't prepared and the originator left the cluster, the previous transaction is rolled-back and
    * a new one is started.
    */
   private Response checkExistingTxForPrepare(PrepareTransactionDecodeContext context) {
      TxState txState = context.getTxState();
      if (txState == null) {
         return null;
      }
      switch (txState.status()) {
         case Status.STATUS_ACTIVE:
            break;
         case Status.STATUS_PREPARED:
            return createTransactionResponse(header, XAResource.XA_OK);
         case Status.STATUS_ROLLEDBACK:
            return createTransactionResponse(header, XAException.XA_RBROLLBACK);
         case Status.STATUS_COMMITTED:
            //weird case. the tx is committed but we received a prepare request?
            return createTransactionResponse(header, XAResource.XA_OK);
         default:
            throw new IllegalStateException();
      }
      if (context.isAlive(txState.getOriginator())) {
         //transaction started on another node but the node is still in the topology. 2 possible scenarios:
         // #1, the topology isn't updated
         // #2, the client timed-out waiting for the reply
         //in any case, we send a ignore reply and the client is free to retry (or rollback)
         return decoder.createNotExecutedResponse(header, null);
      } else {
         //node left the cluster while transaction was running or preparing. we are going to abort the other transaction and start a new one.
         context.rollbackRemoteTransaction();
      }
      return null;
   }

   ErrorResponse createExceptionResponse(Throwable e) {
      if (e instanceof InvalidMagicIdException) {
         log.exceptionReported(e);
         return new ErrorResponse((byte) 0, 0, "", (short) 1, OperationStatus.InvalidMagicOrMsgId, 0, e.toString());
      } else if (e instanceof HotRodUnknownOperationException) {
         log.exceptionReported(e);
         HotRodUnknownOperationException hruoe = (HotRodUnknownOperationException) e;
         return new ErrorResponse(hruoe.version, hruoe.messageId, "", (short) 1, OperationStatus.UnknownOperation, 0, e.toString());
      } else if (e instanceof UnknownVersionException) {
         log.exceptionReported(e);
         UnknownVersionException uve = (UnknownVersionException) e;
         return new ErrorResponse(uve.version, uve.messageId, "", (short) 1, OperationStatus.UnknownVersion, 0, e.toString());
      } else if (e instanceof RequestParsingException) {
         log.exceptionReported(e);
         String msg = e.getCause() == null ? e.toString() : format("%s: %s", e.getMessage(), e.getCause().toString());
         RequestParsingException rpe = (RequestParsingException) e;
         return new ErrorResponse(rpe.version, rpe.messageId, "", (short) 1, OperationStatus.ParseError, 0, msg);
      } else if (e instanceof IllegalStateException) {
         // Some internal server code could throw this, so make sure it's logged
         log.exceptionReported(e);
         return decoder.createErrorResponse(header, e);
      } else if (decoder != null) {
         return decoder.createErrorResponse(header, e);
      } else {
         log.exceptionReported(e);
         return new ErrorResponse((byte) 0, 0, "", (short) 1, OperationStatus.ServerError, 1, e.toString());
      }
   }

   Response replace() {
      // Avoid listener notification for a simple optimization
      // on whether a new version should be calculated or not.
      byte[] prev = cache.withFlags(Flag.SKIP_LISTENER_NOTIFICATION).get(key);
      if (prev != null) {
         // Generate new version only if key present
         prev = cache.replace(key, (byte[]) operationDecodeContext, buildMetadata());
      }
      if (prev != null)
         return successResp(prev);
      else
         return notExecutedResp(null);
   }

   void obtainCache(EmbeddedCacheManager cacheManager) throws RequestParsingException {
      String cacheName = header.cacheName;
      // Try to avoid calling cacheManager.getCacheNames() if possible, since this creates a lot of unnecessary garbage
      AdvancedCache<byte[], byte[]> cache = server.getKnownCache(cacheName);
      if (cache == null) {
         // Talking to the wrong cache are really request parsing errors
         // and hence should be treated as client errors
         InternalCacheRegistry icr = cacheManager.getGlobalComponentRegistry().getComponent(InternalCacheRegistry.class);
         if (icr.isPrivateCache(cacheName)) {
            throw new RequestParsingException(
                  format("Remote requests are not allowed to private caches. Do no send remote requests to cache '%s'", cacheName),
                  header.version, header.messageId);
         } else if (icr.internalCacheHasFlag(cacheName, InternalCacheRegistry.Flag.PROTECTED)) {
            // We want to make sure the cache access is checked everytime, so don't store it as a "known" cache. More
            // expensive, but these caches should not be accessed frequently
            cache = server.getCacheInstance(cacheName, cacheManager, true, false);
         } else if (!cacheName.isEmpty() && !cacheManager.getCacheNames().contains(cacheName)) {
            throw new CacheNotFoundException(
                  format("Cache with name '%s' not found amongst the configured caches", cacheName),
                  header.version, header.messageId);
         } else {
            cache = server.getCacheInstance(cacheName, cacheManager, true, true);
         }
      }
      this.cache = decoder.getOptimizedCache(header, cache, server.getCacheConfiguration(cacheName));
   }

   void withSubect(Subject subject) {
      this.subject = subject;
      this.cache = cache.withSubject(subject);
   }

   Metadata buildMetadata() {
      return buildMetadata(params.lifespan, params.maxIdle);
   }

   private Metadata buildMetadata(ExpirationParam lifespan, ExpirationParam maxIdle) {
      EmbeddedMetadata.Builder metadata = new EmbeddedMetadata.Builder();
      metadata.version(generateVersion(server.getCacheRegistry(header.cacheName)));
      if (lifespan.duration != ServerConstants.EXPIRATION_DEFAULT) {
         metadata.lifespan(toMillis(lifespan, header));
      }
      if (maxIdle.duration != ServerConstants.EXPIRATION_DEFAULT) {
         metadata.maxIdle(toMillis(maxIdle, header));
      }
      return metadata.build();
   }

   Response get() {
      return createGetResponse(cache.getCacheEntry(key));
   }

   Response getKeyMetadata() {
      CacheEntry<byte[], byte[]> ce = cache.getCacheEntry(key);
      if (ce != null) {
         EntryVersion entryVersion = ce.getMetadata().version();
         long version = extractVersion(entryVersion);
         byte[] v = ce.getValue();
         int lifespan = ce.getLifespan() < 0 ? -1 : (int) ce.getLifespan() / 1000;
         int maxIdle = ce.getMaxIdle() < 0 ? -1 : (int) ce.getMaxIdle() / 1000;
         if (header.op == HotRodOperation.GET_WITH_METADATA) {
            return new GetWithMetadataResponse(header.version, header.messageId, header.cacheName, header.clientIntel,
                  header.op, OperationStatus.Success, header.topologyId, v, version,
                  ce.getCreated(), lifespan, ce.getLastUsed(), maxIdle);
         } else {
            int offset = (Integer) operationDecodeContext;
            return new GetStreamResponse(header.version, header.messageId, header.cacheName, header.clientIntel,
                  header.op, OperationStatus.Success, header.topologyId, v, offset, version,
                  ce.getCreated(), lifespan, ce.getLastUsed(), maxIdle);
         }
      } else {
         if (header.op == HotRodOperation.GET_WITH_METADATA) {
            return new GetWithMetadataResponse(header.version, header.messageId, header.cacheName, header.clientIntel,
                  header.op, OperationStatus.KeyDoesNotExist, header.topologyId);
         } else {
            return new GetStreamResponse(header.version, header.messageId, header.cacheName, header.clientIntel,
                  header.op, OperationStatus.KeyDoesNotExist, header.topologyId);
         }
      }
   }

   static long extractVersion(EntryVersion entryVersion) {
      long version = 0;
      if (entryVersion != null) {
         if (entryVersion instanceof NumericVersion) {
            version = NumericVersion.class.cast(entryVersion).getVersion();
         }
         if (entryVersion instanceof SimpleClusteredVersion) {
            version = SimpleClusteredVersion.class.cast(entryVersion).getVersion();
         }
      }
      return version;
   }

   Response containsKey() {
      if (cache.containsKey(key))
         return successResp(null);
      else
         return notExistResp();
   }

   Response replaceIfUnmodified() {
      CacheEntry<byte[], byte[]> entry = cache.withFlags(Flag.SKIP_LISTENER_NOTIFICATION).getCacheEntry(key);
      if (entry != null) {
         byte[] prev = entry.getValue();
         NumericVersion streamVersion = new NumericVersion(params.streamVersion);
         if (entry.getMetadata().version().equals(streamVersion)) {
            // Generate new version only if key present and version has not changed, otherwise it's wasteful
            boolean replaced = cache.replace(key, prev, (byte[]) operationDecodeContext, buildMetadata());
            if (replaced)
               return successResp(prev);
            else
               return notExecutedResp(prev);
         } else {
            return notExecutedResp(prev);
         }
      } else return notExistResp();
   }

   Response putIfAbsent() {
      byte[] prev = cache.get(key);
      if (prev == null) {
         // Generate new version only if key not present
         prev = cache.putIfAbsent(key, (byte[]) operationDecodeContext, buildMetadata());
      }
      if (prev == null)
         return successResp(null);
      else
         return notExecutedResp(prev);
   }

   Response put() {
      // Get an optimised cache in case we can make the operation more efficient
      byte[] prev = cache.put(key, (byte[]) operationDecodeContext, buildMetadata());
      return successResp(prev);
   }

   EntryVersion generateVersion(ComponentRegistry registry) {
      VersionGenerator cacheVersionGenerator = registry.getVersionGenerator();
      if (cacheVersionGenerator == null) {
         // It could be null, for example when not running in compatibility mode.
         // The reason for that is that if no other component depends on the
         // version generator, the factory does not get invoked.
         NumericVersionGenerator newVersionGenerator = new NumericVersionGenerator()
               .clustered(registry.getComponent(RpcManager.class) != null);
         registry.registerComponent(newVersionGenerator, VersionGenerator.class);
         return newVersionGenerator.generateNew();
      } else {
         return cacheVersionGenerator.generateNew();
      }
   }

   Response remove() {
      byte[] prev = cache.remove(key);
      if (prev != null)
         return successResp(prev);
      else
         return notExistResp();
   }

   Response removeIfUnmodified() {
      CacheEntry<byte[], byte[]> entry = cache.getCacheEntry(key);
      if (entry != null) {
         byte[] prev = entry.getValue();
         NumericVersion streamVersion = new NumericVersion(params.streamVersion);
         if (entry.getMetadata().version().equals(streamVersion)) {
            boolean removed = cache.remove(key, prev);
            if (removed)
               return successResp(prev);
            else
               return notExecutedResp(prev);
         } else {
            return notExecutedResp(prev);
         }
      } else {
         return notExistResp();
      }
   }

   Response clear() {
      cache.clear();
      return successResp(null);
   }

   Response successResp(byte[] prev) {
      return decoder.createSuccessResponse(header, prev);
   }

   Response notExecutedResp(byte[] prev) {
      return decoder.createNotExecutedResponse(header, prev);
   }

   Response notExistResp() {
      return decoder.createNotExistResponse(header);
   }

   Response createGetResponse(CacheEntry<byte[], byte[]> entry) {
      return decoder.createGetResponse(header, entry);
   }

   ComponentRegistry getCacheRegistry(String cacheName) {
      return server.getCacheRegistry(cacheName);
   }

   /**
    * Validates if the value read is still valid and the write operation can proceed.
    */
   private boolean isValid(TransactionWrite write, AdvancedCache<byte[], byte[]> readCache) {
      if (write.skipRead()) {
         if (isTrace) {
            log.tracef("Operation %s wasn't read.", write);
         }
         return true;
      }
      CacheEntry<byte[], byte[]> entry = readCache.getCacheEntry(write.key);
      if (write.wasNonExisting()) {
         if (isTrace) {
            log.tracef("Key didn't exist for operation %s. Entry is %s", write, entry);
         }
         return entry == null || entry.getValue() == null;
      }
      if (isTrace) {
         log.tracef("Checking version for operation %s. Entry is %s", write, entry);
      }
      return entry != null && write.versionRead == extractVersion(entry.getMetadata().version());
   }

   /**
    * Creates a transaction response with the specific xa-code.
    */
   private TransactionResponse createTransactionResponse(HotRodHeader header, int xaReturnCode) {
      return new TransactionResponse(header.version, header.messageId, header.cacheName, header.clientIntel, header.op, OperationStatus.Success, header.topologyId, xaReturnCode);
   }

   static class ExpirationParam {
      final long duration;
      final TimeUnitValue unit;

      ExpirationParam(long duration, TimeUnitValue unit) {
         this.duration = duration;
         this.unit = unit;
      }

      @Override
      public String toString() {
         return "ExpirationParam{duration=" + duration + ", unit=" + unit + '}';
      }
   }

   static class RequestParameters {
      final int valueLength;
      final ExpirationParam lifespan;
      final ExpirationParam maxIdle;
      final long streamVersion;

      RequestParameters(int valueLength, ExpirationParam lifespan, ExpirationParam maxIdle, long streamVersion) {
         this.valueLength = valueLength;
         this.lifespan = lifespan;
         this.maxIdle = maxIdle;
         this.streamVersion = streamVersion;
      }

      @Override
      public String toString() {
         return "RequestParameters{" +
               "valueLength=" + valueLength +
               ", lifespan=" + lifespan +
               ", maxIdle=" + maxIdle +
               ", streamVersion=" + streamVersion +
               '}';
      }
   }

   /**
    * Transforms lifespan pass as seconds into milliseconds following this rule (inspired by Memcached):
    * <p>
    * If lifespan is bigger than number of seconds in 30 days, then it is considered unix time. After converting it to
    * milliseconds, we subtract the current time in and the result is returned.
    * <p>
    * Otherwise it's just considered number of seconds from now and it's returned in milliseconds unit.
    */
   static long toMillis(ExpirationParam param, HotRodHeader h) {
      if (param.duration > 0) {
         long milliseconds = param.unit.toTimeUnit().toMillis(param.duration);
         if (milliseconds > MillisecondsIn30days) {
            long unixTimeExpiry = milliseconds - System.currentTimeMillis();
            return unixTimeExpiry < 0 ? 0 : unixTimeExpiry;
         } else {
            return milliseconds;
         }
      } else {
         return param.duration;
      }
   }
}

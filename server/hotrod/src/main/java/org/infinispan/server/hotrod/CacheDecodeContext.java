package org.infinispan.server.hotrod;

import static java.lang.String.format;

import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import javax.security.auth.Subject;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.marshall.WrappedByteArray;
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.container.versioning.NumericVersionGenerator;
import org.infinispan.container.versioning.VersionGenerator;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.metadata.EmbeddedMetadata;
import org.infinispan.metadata.Metadata;
import org.infinispan.multimap.impl.EmbeddedMultimapCache;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.security.Security;
import org.infinispan.server.core.ServerConstants;
import org.infinispan.server.hotrod.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Invokes operations against the cache based on the state kept during decoding process
 */
public final class CacheDecodeContext {
   private static final long MillisecondsIn30days = TimeUnit.DAYS.toMillis(30);
   private static final Log log = LogFactory.getLog(CacheDecodeContext.class, Log.class);

   private final HotRodServer server;

   // union for cache, multimap...
   Object resource;

   VersionedDecoder decoder;
   HotRodHeader header;
   byte[] key;
   RequestParameters params;
   Object operationDecodeContext;
   Subject subject;

   CacheDecodeContext(HotRodServer server) {
      this.server = server;
   }

   public HotRodHeader getHeader() {
      return header;
   }

   public byte[] getKey() {
      return key;
   }

   public byte[] getValue() {
      return (byte[]) operationDecodeContext;
   }

   public RequestParameters getParams() {
      return params;
   }

   <T> T operationContext(Supplier<T> constructor) {
      T opCtx = operationContext();
      if (opCtx == null) {
         opCtx = constructor.get();
         operationDecodeContext = opCtx;
         return opCtx;
      } else {
         return opCtx;
      }
   }

   <T> T operationContext() {
      //noinspection unchecked
      return (T) operationDecodeContext;
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
         if (e instanceof CacheNotFoundException)
            log.debug(e.getMessage());
         else
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

   void withSubect(Subject subject) {
      this.subject = subject;
   }

   public String getPrincipalName() {
      return subject != null ? Security.getSubjectUserPrincipal(subject).getName() : null;
   }

   Metadata buildMetadata() {
      return buildMetadata(params.lifespan, params.maxIdle);
   }

   Metadata buildMetadata(ExpirationParam lifespan, ExpirationParam maxIdle) {
      EmbeddedMetadata.Builder metadata = new EmbeddedMetadata.Builder();
      metadata.version(generateVersion(server.getCacheRegistry(header.cacheName)));
      if (lifespan.duration != ServerConstants.EXPIRATION_DEFAULT) {
         metadata.lifespan(toMillis(lifespan));
      }
      if (maxIdle.duration != ServerConstants.EXPIRATION_DEFAULT) {
         metadata.maxIdle(toMillis(maxIdle));
      }
      return metadata.build();
   }

   private EntryVersion generateVersion(ComponentRegistry registry) {
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

   ComponentRegistry getCacheRegistry(String cacheName) {
      return server.getCacheRegistry(cacheName);
   }

   public AdvancedCache<byte[], byte[]> cache() {
      return (AdvancedCache<byte[], byte[]>) resource;
   }

   public EmbeddedMultimapCache<WrappedByteArray, WrappedByteArray> multimap() {
      return (EmbeddedMultimapCache<WrappedByteArray, WrappedByteArray>) resource;
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
   private static long toMillis(ExpirationParam param) {
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

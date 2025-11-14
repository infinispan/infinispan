package org.infinispan.server.resp.commands.string;

import static org.infinispan.server.resp.operation.SwitchDbOperation.switchDB;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.server.resp.AclCategory;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.RespTypes;
import org.infinispan.server.resp.RespUtil;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.server.resp.json.EmbeddedJsonCache;
import org.infinispan.server.resp.json.JsonBucket;
import org.infinispan.server.resp.serialization.ResponseWriter;

import io.netty.channel.ChannelHandlerContext;

/**
 * COPY
 *
 * @see <a href="https://redis.io/commands/copy/">COPY</a>
 * @since 16.1
 */
public class COPY extends RespCommand implements Resp3Command {
   public static final byte[] REPLACE_BYTES = "REPLACE".getBytes(StandardCharsets.US_ASCII);
   public static final byte[] DB_BYTES = "DB".getBytes(StandardCharsets.US_ASCII);

   public COPY() {
      super(-2, 1, 1, 1, AclCategory.WRITE.mask() | AclCategory.KEYSPACE.mask() | AclCategory.SLOW.mask());
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler, ChannelHandlerContext ctx, List<byte[]> arguments) {
      if (arguments.size() < 2) {
         return CompletableFuture.failedFuture(new IllegalStateException("Missing arguments"));
      }

      boolean tmpIsReplace = false;
      String tmpDbName = null;
      for (int i = 2; i < arguments.size(); i++) {
         byte[] arg = arguments.get(i);
         if (RespUtil.isAsciiBytesEquals(DB_BYTES, arg)) {
            if (i + 1 == arguments.size()) throw new IllegalArgumentException("No database name provided.");
            tmpDbName = new String(arguments.get(++i), StandardCharsets.US_ASCII);
         } else if(RespUtil.isAsciiBytesEquals(REPLACE_BYTES, arg)) {
            tmpIsReplace = true;
         } else {
            throw new IllegalArgumentException("Unknown argument for COPY operation");
         }
      }

      String dbName = tmpDbName;
      boolean isReplace = tmpIsReplace;

      AdvancedCache<byte[], byte[]> originalCache = handler.cache().getAdvancedCache();
      byte[] copiableKeyBytes = arguments.get(0);
      byte[] newKeyBytes = arguments.get(1);

      MediaType vmt = originalCache.getValueDataConversion().getStorageMediaType();
      return handler.stageToReturn(
            originalCache.withMediaType(MediaType.APPLICATION_OCTET_STREAM, vmt).getCacheEntryAsync(copiableKeyBytes)
            .thenCompose(e -> {
                     if(e != null) {
                        Object value = e.getValue();

                        Cache<byte[], byte[]> cacheToCopyTo = originalCache;
                        if (dbName != null) {
                           cacheToCopyTo = handler.respServer().getCacheManager().getCache(dbName);
                        }

                        if (RespTypes.fromValueClass(e.getValue().getClass()) == RespTypes.json) {
                           EmbeddedJsonCache ejc = handler.getJsonCache();
                           if (dbName != null) {
                              switchDB(handler, dbName, ctx);
                              ejc = handler.getJsonCache();
                           }
                           return ejc.set(newKeyBytes, ((JsonBucket) value).value(), "$".getBytes(), !isReplace, false)
                                 .thenCompose(result -> {
                                    if (dbName != null) {
                                       switchDB(handler, originalCache.getName(), ctx);
                                    }
                                    return CompletableFuture.completedFuture("OK".equals(result) ? 1 : 0);
                                 });
                        } else {
                           return (isReplace
                                 ? cacheToCopyTo.putAsync(newKeyBytes, (byte[]) value)
                                 .thenApply(prev -> 1)
                                 : cacheToCopyTo.putIfAbsentAsync(newKeyBytes, (byte[]) value)
                                 .thenApply(prev -> prev != null ? 0 : 1)
                           );
                        }
                     } else  {
                        return CompletableFuture.completedFuture(0);
                     }
                  }
            ), ctx, ResponseWriter.INTEGER);
   }

}

package org.infinispan.server.resp.commands.hash;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.multimap.impl.EmbeddedMultimapPairCache;
import org.infinispan.server.resp.AclCategory;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.RespUtil;
import org.infinispan.server.resp.commands.ArgumentUtils;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.server.resp.serialization.ResponseWriter;

import io.netty.channel.ChannelHandlerContext;

/**
 * HGETDEL key FIELDS numfields field [field ...]
 * <p>
 * Atomically returns the values of the given fields and removes them from the hash. Missing fields reply
 * {@code nil}. The key is removed when its last field is removed.
 *
 * @see <a href="https://redis.io/commands/hgetdel/">HGETDEL</a>
 * @since 16.4
 */
public class HGETDEL extends RespCommand implements Resp3Command {
   private static final byte[] FIELDS = "FIELDS".getBytes(StandardCharsets.US_ASCII);

   public HGETDEL() {
      super(-5, 1, 1, 1, AclCategory.WRITE.mask() | AclCategory.HASH.mask() | AclCategory.FAST.mask());
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler, ChannelHandlerContext ctx,
                                                      List<byte[]> arguments) {
      if (!RespUtil.isAsciiBytesEquals(FIELDS, arguments.get(1))) {
         handler.writer().customError("Mandatory argument FIELDS is missing or not at the right position");
         return handler.myStage();
      }

      long numFields;
      try {
         numFields = ArgumentUtils.toLong(arguments.get(2));
      } catch (NumberFormatException e) {
         numFields = 0;
      }
      if (numFields <= 0) {
         handler.writer().customError("Number of fields must be a positive integer");
         return handler.myStage();
      }
      List<byte[]> fields = arguments.subList(3, arguments.size());
      if (numFields != fields.size()) {
         handler.writer().customError("The `numfields` parameter must match the number of arguments");
         return handler.myStage();
      }

      EmbeddedMultimapPairCache<byte[], byte[], byte[]> multimap = handler.getHashMapMultimap();
      return handler.stageToReturn(multimap.getAndRemove(arguments.get(0), fields), ctx, ResponseWriter.ARRAY_BULK_STRING);
   }
}

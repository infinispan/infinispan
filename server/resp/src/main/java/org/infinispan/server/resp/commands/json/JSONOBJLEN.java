package org.infinispan.server.resp.commands.json;

import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.function.BiConsumer;

import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.RespUtil;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.server.resp.json.EmbeddedJsonCache;
import org.infinispan.server.resp.json.JSONUtil;
import org.infinispan.server.resp.serialization.Resp3Type;
import org.infinispan.server.resp.serialization.ResponseWriter;

import io.netty.channel.ChannelHandlerContext;

/**
 * JSON.OBJLEN
 *
 * @see <a href="https://redis.io/commands/json.objlen/">JSON.OBJLEN</a>
 *
 * @since 15.2
 */
public class JSONOBJLEN extends RespCommand implements Resp3Command {

   private byte[] DEFAULT_PATH = { '.' };

   public JSONOBJLEN() {
      super("JSON.OBJLEN", -2, 1, 1, 1);
   }

   @Override
   public long aclMask() {
      return 0;
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler, ChannelHandlerContext ctx,
         List<byte[]> arguments) {
      byte[] key = arguments.get(0);
      // To keep compatibility, considering the first path only. Additional args will
      // be ignored
      // If missing, default path '.' is used, it's in legacy style, i.e. not jsonpath
      byte[] path = arguments.size() > 1 ? arguments.get(1): DEFAULT_PATH;
      byte[] jsonPath  = JSONUtil.toJsonPath(path);
      boolean withPath = path == jsonPath;
      EmbeddedJsonCache ejc = handler.getJsonCache();
      CompletionStage<List<Long>> cs = ejc.objlen(key, jsonPath);
      // Retun value depends on some logic:
      // for jsonpath return an array of lenghts for all the matching path
      //    or an error if entry doesn't exists
      // for old legacy path return one length as a Number or nil if entry
      //    doesn't exists.
      // Handling these cases here and keeping JsonObjlenFunction simple
      if (withPath) {
         return handler.stageToReturn(cs, ctx, newArrayOrErrorWriter(jsonPath));
      } else {
         return handler.stageToReturn(cs, ctx, JSONOBJLEN::integerOrNullWriter);
      }
   }
   static BiConsumer<List<Long>, ResponseWriter> newArrayOrErrorWriter(byte[] path) {
      return (c, writer) -> {
         if (c==null || c.size() == 0) {
            writer.error("-ERR Path '"+RespUtil.ascii(path)+"' does not exist or not an object");
         } else {
            writer.array(c, Resp3Type.INTEGER);
         }
      };
   }
   static void integerOrNullWriter(List<Long> c, ResponseWriter w) {
      if (c==null || c.size()==0) {
         w.nulls();
      } else {
         w.integers(c.get(0));
      }
   }
}

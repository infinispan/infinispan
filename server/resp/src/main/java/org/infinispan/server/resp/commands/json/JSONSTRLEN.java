package org.infinispan.server.resp.commands.json;

import io.netty.channel.ChannelHandlerContext;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.server.resp.json.EmbeddedJsonCache;
import org.infinispan.server.resp.json.JSONUtil;
import org.infinispan.server.resp.serialization.Resp3Type;
import org.infinispan.server.resp.serialization.ResponseWriter;

import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.function.BiConsumer;

/**
 * JSON.STRLEN
 *
 * @see <a href="https://redis.io/commands/json.strlen/">JSON.STRLEN</a>
 * @since 15.2
 */
public class JSONSTRLEN extends RespCommand implements Resp3Command {

   private byte[] DEFAULT_PATH = { '.' };

   public JSONSTRLEN() {
      super("JSON.STRLEN", -1, 1, 1, 1);
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
       byte[] path = arguments.size() > 1 ? arguments.get(1) : DEFAULT_PATH;
       byte[] jsonPath = JSONUtil.toJsonPath(path);
       boolean withPath = path == jsonPath;
       EmbeddedJsonCache ejc = handler.getJsonCache();
       CompletionStage<List<Long>> lengths = ejc.strLen(key, jsonPath);

       // Return value depends on some logic:
       // for jsonpath return an array of lengths for all the matching path
       //    or an error if entry doesn't exist
       // for old legacy path return one length as a Number or nil if entry
       //    doesn't exist.
       // Handling these cases here and keeping JsonObjlenFunction simple
       if (withPath) {
           return handler.stageToReturn(lengths, ctx, newArrayOrErrorWriter());
       }
       return handler.stageToReturn(lengths, ctx, JSONSTRLEN::integerOrNullWriter);
   }

    static BiConsumer<List<Long>, ResponseWriter> newArrayOrErrorWriter() {
        return (c, writer) -> {
            if (c == null || c.size() == 0) {
                throw new RuntimeException("could not perform this operation on a key that doesn't exist");
            }
            writer.array(c, Resp3Type.INTEGER);
        };
    }
    static void integerOrNullWriter(List<Long> c, ResponseWriter w) {
        if (c == null || c.size() == 0) {
            w.nulls();
        } else {
            w.integers(c.get(0));
        }
    }
}

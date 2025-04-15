package org.infinispan.server.resp.commands.json;

import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.function.BiConsumer;

import org.infinispan.commons.CacheException;
import org.infinispan.server.resp.AclCategory;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.RespUtil;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.server.resp.json.EmbeddedJsonCache;
import org.infinispan.server.resp.serialization.Resp3Type;
import org.infinispan.server.resp.serialization.ResponseWriter;

import io.netty.channel.ChannelHandlerContext;

/**
 * JSON.OBJKEYS
 *
 * @see <a href="https://redis.io/commands/json.objkeys/">JSON.OBJKEYS</a>
 * @since 15.2
 */
public class JSONOBJKEYS extends RespCommand implements Resp3Command {

   public JSONOBJKEYS() {
      super("JSON.OBJKEYS", -2, 1, 1, 1, AclCategory.JSON.mask() | AclCategory.READ.mask() | AclCategory.SLOW.mask());
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler, ChannelHandlerContext ctx,
                                                      List<byte[]> arguments) {
      JSONCommandArgumentReader.CommandArgs commandArgs = JSONCommandArgumentReader.readCommandArgs(arguments);
      EmbeddedJsonCache ejc = handler.getJsonCache();
      CompletionStage<List<List<byte[]>>> result = ejc.objKeys(commandArgs.key(), commandArgs.jsonPath());
      return (commandArgs.isLegacy()) ? handler.stageToReturn(result, ctx, newIntegerOrErrorWriter(commandArgs.jsonPath()))
            : handler.stageToReturn(result, ctx, JSONOBJKEYS::arrayOfArrayWriter);
   }

   static BiConsumer<List<List<byte[]>>, ResponseWriter> newIntegerOrErrorWriter(byte[] path) {
      /*
       * legacy path compatibility returns just one result and it must be not null if first element
       * is null, an error is thrown if list is empty return null
       */
      return (c, writer) -> {
         if (c == null || c.size() == 0) {
            writer.nulls();
            return;
         }
         if (c.get(0) == null) {
            throw new CacheException("Path '" + RespUtil.ascii(path) + "' does not exist or not an object");
         }
         writer.array(c.get(0), Resp3Type.BULK_STRING);
      };
   }

   private static void arrayOfArrayWriter(List<List<byte[]>> lists, ResponseWriter writer) {
      writer.array(lists, (c, writer2) -> {
         if (c == null) {
            writer2.nulls();
         } else {
            writer2.array(c, Resp3Type.BULK_STRING);
         }
      });
   }
}

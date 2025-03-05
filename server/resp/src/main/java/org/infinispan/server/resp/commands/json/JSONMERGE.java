package org.infinispan.server.resp.commands.json;

import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.function.BiConsumer;

import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.server.resp.commands.json.JSONCommandArgumentReader.CommandArgs;
import org.infinispan.server.resp.json.EmbeddedJsonCache;
import org.infinispan.server.resp.json.JSONUtil;
import org.infinispan.server.resp.serialization.ResponseWriter;

import io.netty.channel.ChannelHandlerContext;

/**
 * JSON.MERGE
 *
 * @see <a href="https://redis.io/commands/json.merge/">JSON.MERGE</a>
 *
 *      Note: this version allows to append an element at the end of an array while Redis
 *      implementation returns error
 * @since 15.2
 */
public class JSONMERGE extends RespCommand implements Resp3Command {

   private static final BiConsumer<? super String, ResponseWriter> biConsumer = JSONMERGE::jsonSetBiConsumer;

   public JSONMERGE() {
      super("JSON.MERGE", 4, 1, 1, 1);
   }

   @Override
   public long aclMask() {
      return 0;
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler, ChannelHandlerContext ctx,
                                                      List<byte[]> arguments) {
      CommandArgs args = JSONCommandArgumentReader.readCommandArgs(arguments);
      byte[] value = arguments.get(2);
      EmbeddedJsonCache ejc = handler.getJsonCache();
      if (JSONUtil.isValueInvalid(value)) {
         handler.writer().customError("Invalid json value for JSON.MERGE");
         return handler.myStage();
      }
      CompletionStage<String> cs = ejc.merge(args.key(), args.jsonPath(), value);
      return handler.stageToReturn(cs, ctx, biConsumer);
   }

   private static void jsonSetBiConsumer(String value, ResponseWriter writer) {
      if (value == null) {
         // Use null consumer
         writer.nulls();
         return;
      }
      if ("OK".equals(value)) {
         // Use ok consumer
         writer.ok();
         return;
      }
      // All the rest are errors
      writer.error(value);
   }

}

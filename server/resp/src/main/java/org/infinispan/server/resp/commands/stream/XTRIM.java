package org.infinispan.server.resp.commands.stream;

import static org.infinispan.server.resp.commands.stream.StreamCommandUtils.parseTrimSpec;

import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.server.resp.AclCategory;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.server.resp.commands.stream.StreamCommandUtils.TrimSpec;
import org.infinispan.server.resp.exception.RespCommandException;
import org.infinispan.server.resp.serialization.ResponseWriter;
import org.infinispan.server.resp.stream.StreamTrimFunction;

import io.netty.channel.ChannelHandlerContext;

/**
 * XTRIM
 *
 * @see <a href="https://redis.io/commands/xtrim/">XTRIM</a>
 * @since 16.4
 */
public class XTRIM extends RespCommand implements Resp3Command {

   public XTRIM() {
      super(-4, 1, 1, 1, AclCategory.WRITE.mask() | AclCategory.STREAM.mask() | AclCategory.SLOW.mask());
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler, ChannelHandlerContext ctx,
         List<byte[]> arguments) {
      // XTRIM key <MAXLEN | MINID> [= | ~] threshold [LIMIT count]
      byte[] key = arguments.get(0);
      TrimSpec trim = parseTrimSpec(arguments, 1);
      if (trim.nextPos() != arguments.size()) {
         throw new RespCommandException("ERR syntax error");
      }

      StreamTrimFunction function = new StreamTrimFunction(trim.strategy(), trim.maxlen(), trim.minId(),
            trim.limit());
      return handler.stageToReturn(handler.getStreamCache().trim(key, function), ctx, ResponseWriter.INTEGER);
   }
}

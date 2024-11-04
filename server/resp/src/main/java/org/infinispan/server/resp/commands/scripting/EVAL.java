package org.infinispan.server.resp.commands.scripting;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.server.resp.AclCategory;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.ArgumentUtils;
import org.infinispan.server.resp.commands.Resp3Command;

import io.netty.channel.ChannelHandlerContext;

/**
 * EVAL
 * <p>
 *    In Redis, use of EVAL is discouraged for repeated invocations of the same script. Redis stores such scripts
 *    in a bounded cache to avoid unlimited growth and recommends using the <code>SCRIPT LOAD</code> in combination with
 *    <code>EVALSHA</code> and <code>EVALSHA_RO</code> instead.
 *    Infinispan doesn't cache scripts
 * </p>
 *
 * @see <a href="https://redis.io/docs/latest/commands/eval/">EVAL</a>
 * @since 15.1
 */
public class EVAL extends RespCommand implements Resp3Command {

   public EVAL() {
      super(-3, 1, 1, 1);
   }

   @Override
   public long aclMask() {
      return AclCategory.SCRIPTING | AclCategory.SLOW;
   }

   @Override
   public final CompletionStage<RespRequestHandler> perform(Resp3Handler handler,
                                                            ChannelHandlerContext ctx,
                                                            List<byte[]> arguments) {
      String script = new String(arguments.get(0), StandardCharsets.US_ASCII);
      int numKeys = (int) ArgumentUtils.toLong(arguments.get(1));
      String[] keys = new String[numKeys];
      for (int i = 0; i < numKeys; i++) {
         keys[i] = new String(arguments.get(i + 2), StandardCharsets.US_ASCII);
      }
      String[] argv = new String[arguments.size() - numKeys - 2];
      for (int i = numKeys; i < arguments.size() - 2; i++) {
         argv[i - numKeys] = new String(arguments.get(i + 2), StandardCharsets.US_ASCII);
      }
      return performEval(handler, ctx, script, keys, argv);
   }

   protected CompletionStage<RespRequestHandler> performEval(Resp3Handler handler, ChannelHandlerContext ctx, String script, String[] keys, String[] argv) {
      try {
         handler.respServer().luaEngine().eval(handler, ctx, script, keys, argv, 0);
         return handler.myStage();
      } catch (Exception e) {
         handler.writer().customError(e.getMessage()); // FIXME
         return handler.myStage();
      }
   }
}

package org.infinispan.server.resp.commands.stream;

import static org.infinispan.server.resp.commands.stream.StreamCommandUtils.TRIM_NONE;
import static org.infinispan.server.resp.commands.stream.StreamCommandUtils.isToken;
import static org.infinispan.server.resp.commands.stream.StreamCommandUtils.matches;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.server.resp.AclCategory;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.RespUtil;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.server.resp.commands.stream.StreamCommandUtils.TrimSpec;
import org.infinispan.server.resp.exception.RespCommandException;
import org.infinispan.server.resp.stream.StreamAddFunction;
import org.infinispan.server.resp.stream.StreamID;

import io.netty.channel.ChannelHandlerContext;

/**
 * XADD
 *
 * <p>
 * Appends the specified stream entry to the stream at the specified key. If the key does not exist, a new stream is
 * created, unless the {@code NOMKSTREAM} option is utilized. The entry ID is auto-generated with {@code *}, or
 * partially generated with the {@code <ms>-*} format.
 * </p>
 *
 * @see <a href="https://redis.io/commands/xadd/">XADD</a>
 * @since 16.4
 */
public class XADD extends RespCommand implements Resp3Command {

   private static final byte[] NOMKSTREAM = "NOMKSTREAM".getBytes(StandardCharsets.US_ASCII);
   private static final byte[] MAXLEN = "MAXLEN".getBytes(StandardCharsets.US_ASCII);
   private static final byte[] MINID = "MINID".getBytes(StandardCharsets.US_ASCII);

   public XADD() {
      super(-5, 1, 1, 1, AclCategory.WRITE.mask() | AclCategory.STREAM.mask() | AclCategory.FAST.mask());
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler, ChannelHandlerContext ctx,
         List<byte[]> arguments) {
      // XADD key [NOMKSTREAM] [<MAXLEN | MINID> [= | ~] threshold [LIMIT count]] <* | id> field value [field value ...]
      byte[] key = arguments.get(0);

      boolean nomkstream = false;
      int trimStrategy = TRIM_NONE;
      long maxlen = 0;
      StreamID minId = null;
      long limit = -1;

      int pos = 1;
      while (pos < arguments.size()) {
         byte[] argument = arguments.get(pos);
         if (matches(argument, NOMKSTREAM)) {
            nomkstream = true;
            pos++;
         } else if (matches(argument, MAXLEN) || matches(argument, MINID)) {
            TrimSpec trim = StreamCommandUtils.parseTrimSpec(arguments, pos);
            trimStrategy = trim.strategy();
            maxlen = trim.maxlen();
            minId = trim.minId();
            limit = trim.limit();
            pos = trim.nextPos();
         } else {
            break;
         }
      }

      if (pos >= arguments.size()) {
         throw new RespCommandException("ERR wrong number of arguments for key, value pair");
      }

      // The ID: '*', '<ms>-*' or an explicit '<ms>-<seq>'.
      boolean autoId = false;
      boolean autoSeq = false;
      long idMs = 0;
      long idSeq = 0;
      byte[] idArgument = arguments.get(pos++);
      if (isToken(idArgument, '*')) {
         autoId = true;
      } else {
         String idValue = RespUtil.ascii(idArgument);
         int dash = idValue.indexOf('-');
         if (dash >= 0 && idValue.length() == dash + 2 && idValue.charAt(dash + 1) == '*') {
            // The '<ms>-*' format: the milliseconds part is explicit, the sequence part is generated.
            autoSeq = true;
            idMs = parseUnsigned(idValue.substring(0, dash));
         } else {
            try {
               StreamID id = StreamID.parse(idArgument);
               idMs = id.ms();
               idSeq = id.seq();
            } catch (NumberFormatException e) {
               throw new RespCommandException("ERR Invalid stream ID specified as stream command argument");
            }
         }
      }

      // The field-value pairs: at least one pair, with an even number of arguments.
      int fieldArgs = arguments.size() - pos;
      if (fieldArgs < 2 || fieldArgs % 2 != 0) {
         throw new RespCommandException("ERR wrong number of arguments for key, value pair");
      }
      List<byte[]> fields = new ArrayList<>(fieldArgs);
      for (int i = pos; i < arguments.size(); i++) {
         fields.add(arguments.get(i));
      }

      StreamAddFunction function = new StreamAddFunction(nomkstream, trimStrategy, maxlen, minId, limit,
            autoId, autoSeq, idMs, idSeq, fields, handler.respServer().getTimeService().wallClockTime());
      return handler.stageToReturn(handler.getStreamCache().add(key, function), ctx, (id, writer) -> {
         if (id == null) {
            // The NOMKSTREAM option on a missing key.
            writer.nulls();
         } else {
            writer.string(id);
         }
      });
   }

   private static long parseUnsigned(String value) {
      try {
         return Long.parseUnsignedLong(value);
      } catch (NumberFormatException e) {
         throw new RespCommandException("ERR Invalid stream ID specified as stream command argument");
      }
   }
}

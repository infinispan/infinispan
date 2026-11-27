package org.infinispan.server.resp.commands.stream;

import static org.infinispan.server.resp.commands.stream.StreamCommandUtils.matches;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.server.resp.AclCategory;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.ArgumentUtils;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.server.resp.exception.RespCommandException;
import org.infinispan.server.resp.stream.StreamID;
import org.infinispan.server.resp.stream.StreamSetIdFunction;

import io.netty.channel.ChannelHandlerContext;

/**
 * XSETID
 *
 * @see <a href="https://redis.io/commands/xsetid/">XSETID</a>
 * @since 16.4
 */
public class XSETID extends RespCommand implements Resp3Command {

   private static final byte[] ENTRIESADDED = "ENTRIESADDED".getBytes(StandardCharsets.US_ASCII);
   private static final byte[] MAXDELETEDID = "MAXDELETEDID".getBytes(StandardCharsets.US_ASCII);

   public XSETID() {
      super(-3, 1, 1, 1, AclCategory.WRITE.mask() | AclCategory.STREAM.mask() | AclCategory.FAST.mask());
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler, ChannelHandlerContext ctx,
         List<byte[]> arguments) {
      // XSETID key id [ENTRIESADDED count] [MAXDELETEDID id]
      byte[] key = arguments.get(0);

      StreamID id;
      try {
         id = StreamID.parse(arguments.get(1));
      } catch (NumberFormatException e) {
         throw new RespCommandException("ERR Invalid stream ID specified as stream command argument");
      }

      boolean entriesAddedGiven = false;
      long entriesAdded = 0;
      boolean maxDeletedGiven = false;
      StreamID maxDeletedId = StreamID.ZERO;

      int pos = 2;
      while (pos < arguments.size()) {
         if (matches(arguments.get(pos), ENTRIESADDED)) {
            if (++pos >= arguments.size()) {
               throw new RespCommandException("ERR syntax error");
            }
            entriesAdded = ArgumentUtils.toLong(arguments.get(pos++));
            if (entriesAdded < 0) {
               throw new RespCommandException("ERR entries_added must be positive");
            }
            entriesAddedGiven = true;
         } else if (matches(arguments.get(pos), MAXDELETEDID)) {
            if (++pos >= arguments.size()) {
               throw new RespCommandException("ERR syntax error");
            }
            try {
               maxDeletedId = StreamID.parse(arguments.get(pos++));
            } catch (NumberFormatException e) {
               throw new RespCommandException("ERR Invalid stream ID specified as stream command argument");
            }
            maxDeletedGiven = true;
         } else {
            // Any other argument is a syntax error, including the ENTRIESADDED and MAXDELETEDID values provided
            // without their keyword.
            throw new RespCommandException("ERR syntax error");
         }
      }

      StreamSetIdFunction function = new StreamSetIdFunction(id, entriesAddedGiven, entriesAdded, maxDeletedGiven,
            maxDeletedId);
      return handler.stageToReturn(handler.getStreamCache().setId(key, function), ctx, (ignore, writer) -> writer.ok());
   }
}

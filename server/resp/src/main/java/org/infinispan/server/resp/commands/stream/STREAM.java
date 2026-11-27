package org.infinispan.server.resp.commands.stream;

import static org.infinispan.server.resp.commands.stream.StreamCommandUtils.matches;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.server.resp.AclCategory;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.RespVersion;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.server.resp.exception.RespCommandException;
import org.infinispan.server.resp.serialization.RespConstants;
import org.infinispan.server.resp.serialization.ResponseWriter;
import org.infinispan.server.resp.stream.StreamBucket;
import org.infinispan.server.resp.stream.StreamEntry;

import io.netty.channel.ChannelHandlerContext;

/**
 * XINFO STREAM
 *
 * <p>
 * Returns general information about the stream. The {@code radix-tree-keys} and {@code radix-tree-nodes} fields are
 * reported with structural equivalents of the flat bucket representation: the number of entries and one node,
 * respectively. The {@code groups} field always reports zero, as consumer groups are not supported yet.
 * </p>
 *
 * @see <a href="https://redis.io/commands/xinfo-stream/">XINFO STREAM</a>
 * @since 16.4
 */
public class STREAM extends RespCommand implements Resp3Command {

   private static final byte[] FULL = "FULL".getBytes(StandardCharsets.US_ASCII);

   public STREAM() {
      // The subcommand receives the arguments with its own name at position zero: [STREAM, key, FULL?]
      super(-3, 0, 0, 0, AclCategory.READ.mask() | AclCategory.STREAM.mask() | AclCategory.SLOW.mask());
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler, ChannelHandlerContext ctx,
         List<byte[]> arguments) {
      byte[] key = arguments.get(1);
      boolean full = arguments.size() > 2;
      if (full) {
         if (arguments.size() > 3 || !matches(arguments.get(2), FULL)) {
            throw new RespCommandException("ERR syntax error");
         }
      }

      return handler.stageToReturn(handler.getStreamCache().bucket(key), ctx, (bucket, writer) -> {
         if (bucket == null) {
            throw new RespCommandException("ERR no such key");
         }
         writeInfo(bucket, full, writer);
      });
   }

   private static void writeInfo(StreamBucket bucket, boolean full, ResponseWriter writer) {
      StreamEntry first = bucket.firstEntry();
      StreamEntry last = bucket.lastEntry();

      // The basic reply has ten key-value pairs, including the first and last entries. The FULL reply replaces them
      // with an entries array containing both.
      int pairs = full ? 9 : 10;
      if (writer.version() == RespVersion.RESP3) {
         writer.writeNumericPrefix(RespConstants.MAP, pairs);
      } else {
         writer.arrayStart(pairs * 2);
      }

      writeField("length", writer);
      writer.integers(bucket.size());

      writeField("radix-tree-keys", writer);
      writer.integers(bucket.size());

      writeField("radix-tree-nodes", writer);
      writer.integers(1);

      writeField("last-generated-id", writer);
      writer.string(bucket.lastId().asBytes());

      writeField("max-deleted-entry-id", writer);
      writer.string(bucket.maxDeletedId().asBytes());

      writeField("entries-added", writer);
      writer.integers(bucket.entriesAdded());

      writeField("recorded-first-entry-id", writer);
      writer.string(bucket.recordedFirstId().asBytes());

      writeField("groups", writer);
      writer.integers(0);

      if (full) {
         writeField("entries", writer);
         writer.arrayStart(2);
         writeEntryOrNull(first, writer);
         writeEntryOrNull(last, writer);
         writer.arrayEnd();
      } else {
         writeField("first-entry", writer);
         writeEntryOrNull(first, writer);

         writeField("last-entry", writer);
         writeEntryOrNull(last, writer);
      }
   }

   private static void writeField(String name, ResponseWriter writer) {
      writer.string(name.getBytes(StandardCharsets.US_ASCII));
   }

   private static void writeEntryOrNull(StreamEntry entry, ResponseWriter writer) {
      if (entry == null) {
         writer.nulls();
      } else {
         StreamCommandUtils.ENTRY_SERIALIZER.accept(entry, writer);
      }
   }
}

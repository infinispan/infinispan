package org.infinispan.server.memcached.text;

import java.nio.charset.StandardCharsets;

import io.netty.buffer.ByteBuf;

/**
 * @since 15.0
 **/
public enum TextCommand {
   // Text commands
   set,
   get,
   gets,
   add,
   delete,
   replace,
   append,
   prepend,
   incr,
   decr,
   gat,
   gats,
   touch,
   flush_all,
   cas,
   stats,
   verbosity,
   version,
   quit,
   // Meta commands
   mg,
   ms,
   md,
   ma,
   mn,
   me;

   private final byte[] identifier;

   TextCommand() {
      this.identifier = name().getBytes(StandardCharsets.US_ASCII);
   }

   private static final TextCommand[] VALUES = values();

   public static TextCommand valueOf(ByteBuf b) {
      int offset = b.readerIndex();

      Candidate: for (TextCommand cmd : VALUES) {
         byte[] candidate = cmd.identifier;
         if (candidate.length != b.readableBytes()) continue;

         for (int i = 0; i < candidate.length; i++) {
            byte l = candidate[i];
            byte r = b.getByte(offset + i);
            if (l != r && l != (r + 32)) continue Candidate;
         }

         return cmd;
      }

      throw new IllegalArgumentException("Unknown command: " + b.toString(StandardCharsets.US_ASCII));
   }
}

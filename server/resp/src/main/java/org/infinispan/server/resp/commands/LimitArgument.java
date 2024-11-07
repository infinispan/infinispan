package org.infinispan.server.resp.commands;

import java.util.List;

import org.infinispan.server.resp.Resp3Handler;

public final class LimitArgument {
   public long offset;
   public long count;
   public boolean error;
   public int nextArgPos;

   public static LimitArgument parse(Resp3Handler handler, List<byte[]> arguments, int pos) {
      LimitArgument limitArgument = new LimitArgument();
      try {
         limitArgument.offset = ArgumentUtils.toLong(arguments.get(pos));
         limitArgument.count = ArgumentUtils.toLong(arguments.get(pos + 1));
      } catch (NumberFormatException ex) {
         handler.writer().valueNotInteger();
         limitArgument.error = true;
      } catch (IndexOutOfBoundsException ex) {
         handler.writer().syntaxError();
         limitArgument.error = true;
      }
      limitArgument.nextArgPos = pos + 2;
      return limitArgument;
   }
}

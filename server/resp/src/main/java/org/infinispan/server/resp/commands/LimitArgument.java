package org.infinispan.server.resp.commands;

import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespErrorUtil;

import java.util.List;

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
         RespErrorUtil.valueNotInteger(handler.allocator());
         limitArgument.error = true;
      } catch (IndexOutOfBoundsException ex) {
         RespErrorUtil.syntaxError(handler.allocator());
         limitArgument.error = true;
      }
      limitArgument.nextArgPos = pos + 2;
      return limitArgument;
   }
}

package org.infinispan.server.resp.commands.list.blocking;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;

import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespErrorUtil;
import org.infinispan.server.resp.Util;
import org.infinispan.server.resp.commands.ArgumentUtils;

public class BLMPOP extends AbstractBlockingPop {
   private static final byte[] LEFT = new byte[] { 'L', 'E', 'F', 'T' };
   private static final byte[] RIGHT = new byte[] { 'R', 'I', 'G', 'H', 'T'};

   public BLMPOP() {
      super(-5, 0, 0, 0);
   }

   @Override
   PopConfiguration parseArguments(Resp3Handler handler, List<byte[]> arguments) {
      double timeout = ArgumentUtils.toDouble(arguments.get(0));
      if (timeout < 0) {
         RespErrorUtil.mustBePositive(handler.allocator(), "timeout");
         return null;
      }

      int numKeys = ArgumentUtils.toInt(arguments.get(1));
      int additionalArgs = numKeys + 2;
      List<byte[]> keys = arguments.subList(2, additionalArgs);
      boolean head = isHead(arguments.get(additionalArgs++));

      int count = -1;

      // Last two arguments, `COUNT <number>`.
      if (arguments.size() == additionalArgs + 2) {
         count = ArgumentUtils.toInt(arguments.get(++additionalArgs));

         if (count <= 0) {
            RespErrorUtil.mustBePositive(handler.allocator(), "count");
            return null;
         }
      }

      return new PopConfiguration(head, count == -1 ? numKeys : count, (long) (timeout * Duration.ofSeconds(1).toMillis()), keys);
   }

   private boolean isHead(byte[] bytes) {
      if (Util.isAsciiBytesEquals(LEFT, bytes))
         return true;

      if (!Util.isAsciiBytesEquals(RIGHT, bytes))
         throw new IllegalArgumentException("Unknown argument: " + Arrays.toString(bytes));

      return false;
   }
}

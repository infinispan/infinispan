package org.infinispan.server.resp.commands.list.blocking;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;

import org.infinispan.server.resp.AclCategory;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespUtil;
import org.infinispan.server.resp.commands.ArgumentUtils;

/**
 * BLMPOP
 *
 * @see <a href="https://redis.io/commands/blmpop/">BLMPOP</a>
 * @since 15.0
 */
public class BLMPOP extends AbstractBlockingPop {
   private static final byte[] LEFT = new byte[] { 'L', 'E', 'F', 'T' };
   private static final byte[] RIGHT = new byte[] { 'R', 'I', 'G', 'H', 'T'};

   public BLMPOP() {
      super(-5, 0, 0, 0, AclCategory.WRITE.mask() | AclCategory.LIST.mask() | AclCategory.SLOW.mask() | AclCategory.BLOCKING.mask());
   }

   @Override
   PopConfiguration parseArguments(Resp3Handler handler, List<byte[]> arguments) {
      double timeout = ArgumentUtils.toDouble(arguments.get(0));
      if (timeout < 0) {
         handler.writer().mustBePositive("timeout");
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
            handler.writer().mustBePositive("count");
            return null;
         }
      }

      return new PopConfiguration(head, count == -1 ? numKeys : count, (long) (timeout * Duration.ofSeconds(1).toMillis()), keys);
   }

   private boolean isHead(byte[] bytes) {
      if (RespUtil.isAsciiBytesEquals(LEFT, bytes))
         return true;

      if (!RespUtil.isAsciiBytesEquals(RIGHT, bytes))
         throw new IllegalArgumentException("Unknown argument: " + Arrays.toString(bytes));

      return false;
   }
}

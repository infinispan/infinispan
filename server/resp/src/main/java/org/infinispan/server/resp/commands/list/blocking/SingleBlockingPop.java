package org.infinispan.server.resp.commands.list.blocking;

import java.time.Duration;
import java.util.List;

import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.commands.ArgumentUtils;

/**
 * Base class for the blocking pop commands with the {@code key [key ...] timeout} syntax.
 *
 * @param <R> the type of a successful pop result
 */
public abstract class SingleBlockingPop<R> extends AbstractBlockingPop<R> {

   private final boolean head;

   protected SingleBlockingPop(Source<R> source, boolean head, int arity, int firstKeyPos, int lastKeyPos, int steps, long aclMask) {
      super(source, arity, firstKeyPos, lastKeyPos, steps, aclMask);
      this.head = head;
   }

   @Override
   protected PopConfiguration parseArguments(Resp3Handler handler, List<byte[]> arguments) {
      int lastKeyIdx = arguments.size() - 1;
      List<byte[]> filterKeys = arguments.subList(0, lastKeyIdx);
      double argTimeout = ArgumentUtils.toDouble(arguments.get(lastKeyIdx));
      // Using last arg as timeout if it can be a double
      if (argTimeout < 0) {
         handler.writer().mustBePositive();
         return null;
      }
      long timeout = (long) (argTimeout * Duration.ofSeconds(1).toMillis());

      return new PopConfiguration(head, 1, timeout, filterKeys);
   }
}

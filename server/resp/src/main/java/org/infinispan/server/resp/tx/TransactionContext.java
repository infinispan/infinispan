package org.infinispan.server.resp.tx;

import io.netty.channel.ChannelHandlerContext;
import io.netty.util.AttributeKey;

/**
 * Delimit the context of commands run by an {@link org.infinispan.server.resp.commands.tx.EXEC} command.
 * <p>
 * In a transaction execution, the commands are queued and executed (in order) after receiving an {@link org.infinispan.server.resp.commands.tx.EXEC}
 * command. This context provides delimitation of the transaction execution so commands are aware whether they are running
 * from inside a transaction.
 * </p>
 *
 * @since 15.1
 */
public final class TransactionContext {

   private static final AttributeKey<Boolean> TRANSACTIONAL_CONTEXT = AttributeKey.newInstance("multi-exec");

   private TransactionContext() { }

   /**
    * Start the transaction context.
    *
    * @param ctx The client context executing the transaction.
    * @throws IllegalStateException in case another context is in place.
    */
   public static void startTransactionContext(ChannelHandlerContext ctx) {
      Boolean existing =  ctx.channel().attr(TRANSACTIONAL_CONTEXT).setIfAbsent(Boolean.TRUE);
      if (existing != null)
         throw new IllegalStateException("Nested transaction context");
   }

   /**
    * Finish the transaction context.
    *
    * @param ctx The client context executing the transaction.
    * @throws IllegalStateException in case no transaction context is in place.
    */
   public static void endTransactionContext(ChannelHandlerContext ctx) {
      Boolean existing = ctx.channel().attr(TRANSACTIONAL_CONTEXT).getAndSet(null);
      if (existing == null)
         throw new IllegalStateException("Not transaction context to remove");
   }

   /**
    * Verify whether the current client is in a transactional context.
    *
    * @param ctx The client context to verify.
    * @return <code>true</code> if running from a transaction, <code>false</code>, otherwise.
    */
   public static boolean isInTransactionContext(ChannelHandlerContext ctx) {
      return Boolean.TRUE.equals(ctx.channel().attr(TRANSACTIONAL_CONTEXT).get());
   }
}

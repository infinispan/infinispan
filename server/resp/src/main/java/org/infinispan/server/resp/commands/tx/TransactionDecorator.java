package org.infinispan.server.resp.commands.tx;

import java.util.concurrent.CompletionStage;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.cache.impl.AbstractDelegatingAdvancedCache;
import org.infinispan.cache.impl.CacheImpl;
import org.infinispan.cache.impl.DecoratedCache;
import org.infinispan.commons.tx.DefaultResourceConverter;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.LocalTxInvocationContext;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.transaction.impl.LocalTransaction;
import org.infinispan.transaction.impl.TransactionTable;
import org.infinispan.transaction.tm.EmbeddedTransaction;
import org.infinispan.transaction.tm.EmbeddedTransactionManager;

/**
 * Decorates the cache to utilize a transactional context.
 *
 * <p>
 * The {@link EXEC} command execute many queued commands. These commands might change threads during the execution.
 * Before executing the commands, we decorate the cache to bypass the creation of the {@link InvocationContext}. This
 * way, we guarantee that all commands execute within the same transaction. Otherwise, we could lose context due to thread
 * changes.
 * </p>
 *
 * <p>
 * We extend the {@link DecoratedCache} to override the methods to utilize a transactional context. We inject the cache
 * in the chain of delegates just before the final link to submit commands ({@link CacheImpl}). This guarantees that
 * the proper chain of command exist and it only affects the {@link InvocationContext} creation.
 * </p>
 *
 * @since 15.0
 * @author José Bolina
 */
final class TransactionDecorator {

   private TransactionDecorator() { }

   /**
    * Begin a new transaction with the given gaven for the given handler.
    *
    * <p>
    * A transaction is initiated and it will contain all submitted commands until
    * {@link #completeTransaction(TransactionResume, boolean)} is invoked. After the transaction starts, the user's
    * {@link Resp3Handler} should <b>not</b> be utilized for other commands that do not belong to the transaction.
    * </p>
    *
    * @param handler The handler to enter into transactional context.
    * @param cache The cache to utilize for operations.
    * @return A new instance of {@link TransactionResume} to utilize to complete the transaction.
    * @throws IllegalStateException If a {@link CacheImpl} is not found.
    */
   static TransactionResume beginTransaction(Resp3Handler handler, AdvancedCache<byte[], byte[]> cache) {
      EmbeddedTransaction tx = new EmbeddedTransaction(EmbeddedTransactionManager.getInstance());
      TransactionTable table = acquireTransactionTable(cache);
      LocalTransaction localTx = table.getOrCreateLocalTransaction(tx, false);
      LocalTxInvocationContext ic = new LocalTxInvocationContext(localTx);
      table.enlistClientTransaction(tx, localTx);
      handler.setCache(injectExecCache(cache, ic));
      return new TransactionResume(handler, cache, tx);
   }

   /**
    * Completes the transaction and frees the handler and cache.
    *
    * <p>
    * Commits/rollbacks the transaction and release the {@link Resp3Handler} and {@link AdvancedCache} provided to
    * start the transaction.
    * </p>
    *
    * @param resume The transaction context created to start the transaction.
    * @param success <code>true</code> to commit the transaction, <code>false</code>, otherwise.
    * @return A {@link CompletionStage} that completes after the transaction is commit/rollback.
    */
   static CompletionStage<Void> completeTransaction(TransactionResume resume, boolean success) {
      resume.handler.setCache(resume.cache);
      return resume.tx.runCommitAsync(!success, DefaultResourceConverter.INSTANCE);
   }

   private static TransactionTable acquireTransactionTable(AdvancedCache<?, ?> cache) {
      return ComponentRegistry.componentOf(cache, TransactionTable.class);
   }

   /**
    * Intercepts the chain of caches.
    *
    * <p>
    * Given a chain of caches ({@link AbstractDelegatingAdvancedCache}), inserts the {@link RespExecDecoratedCache} just
    * above the {@link CacheImpl} and rebuilds the chain. This guarantees the delegation chain will maintain the same order,
    * and even additional chances of encoding or authentication will keep the bottom order intact.
    * </p>
    *
    * <p>
    * The method will traverse recursively until finding the {@link CacheImpl} instance, and will rebuild again backwards
    * with the {@link AbstractDelegatingAdvancedCache#rewrap(AdvancedCache)} method. For instance, the chain
    * <code>Delegate1</code> -> <code>Delegate2</code> -> <code>CacheImpl</code> will transform into <code>Delegate1</code>
    * -> <code>Delegate2</code> -> <code>RespExecDecoratedCache</code> -> <code>CacheImpl</code>.
    * </p>
    *
    * @param cache Instance to verify or rebuild the chain.
    * @param ctx Context to utilize in the {@link RespExecDecoratedCache}.
    * @return A new delegation chain with {@link RespExecDecoratedCache} injected.
    * @throws IllegalStateException if a {@link CacheImpl} does not exist in the chain.
    */
   private static AdvancedCache<byte[], byte[]> injectExecCache(Cache<byte[], byte[]> cache, InvocationContext ctx) {
      if (cache instanceof CacheImpl<byte[], byte[]> ci)
         return new RespExecDecoratedCache(ctx, ci, 0);

      if (cache instanceof AbstractDelegatingAdvancedCache<byte[],byte[]> adac) {
         AdvancedCache<byte[], byte[]> delegate = injectExecCache(adac.getDelegate(), ctx);
         return adac.rewrap(delegate);
      }

      throw new IllegalStateException("Simple cache not found for: " + cache.getClass());
   }

   /**
    * Transactional context to complete a transaction and release resources.
    */
   static final class TransactionResume {
      private final Resp3Handler handler;
      private final AdvancedCache<byte[], byte[]> cache;
      private final EmbeddedTransaction tx;

      TransactionResume(Resp3Handler handler, AdvancedCache<byte[], byte[]> cache, EmbeddedTransaction tx) {
         this.handler = handler;
         this.cache = cache;
         this.tx = tx;
      }
   }

   /**
    * Decorates the cache to maintain the transactional context throughout many commands.
    *
    * <p>
    * Re-wrapping or adding flags must ensure the order with the {@link org.infinispan.cache.impl.EncoderCache}. The
    * encoding of keys and values must <b>always</b> happen before invoking the command with the decorated cache.
    * </p>
    */
   private static class RespExecDecoratedCache extends DecoratedCache<byte[], byte[]> {
      private final InvocationContext ctx;

      private RespExecDecoratedCache(InvocationContext ctx, CacheImpl<byte[], byte[]> delegate, long flags) {
         super(delegate, flags);
         this.ctx = ctx;
      }

      private RespExecDecoratedCache(InvocationContext ctx, CacheImpl<byte[], byte[]> delegate, Object lockOwner, long flags) {
         super(delegate, lockOwner, flags);
         this.ctx = ctx;
      }

      @Override
      protected DecoratedCache<byte[], byte[]> newInstance(CacheImpl<byte[], byte[]> impl, Object lockOwner, long newFlags) {
         return new RespExecDecoratedCache(ctx, impl, lockOwner, newFlags);
      }

      @Override
      protected InvocationContext readContext(int size) {
         return ctx;
      }

      @Override
      protected InvocationContext writeContext(int size) {
         return ctx;
      }

      @Override
      public boolean bypassInvocationContextFactory() {
         return true;
      }
   }
}

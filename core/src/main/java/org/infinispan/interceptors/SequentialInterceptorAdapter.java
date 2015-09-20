package org.infinispan.interceptors;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.commons.CacheException;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.interceptors.base.BaseSequentialInterceptor;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.remoting.transport.Address;
import org.infinispan.transaction.impl.AbstractCacheTransaction;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import javax.transaction.Transaction;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;

/**
 * @author Dan Berindei
 * @since 8.0
 */
public class SequentialInterceptorAdapter extends BaseSequentialInterceptor {
   private static final Log log = LogFactory.getLog(SequentialInterceptorAdapter.class);
   private static final boolean trace = log.isTraceEnabled();

   private final CommandInterceptor adaptedInterceptor;

   // TODO Inject an existing thread pool
   final static ExecutorService executor =
         Executors.newCachedThreadPool(LegacyInterceptorThreadFactory.INSTANCE);

   public SequentialInterceptorAdapter(CommandInterceptor adaptedInterceptor) {
      this.adaptedInterceptor = adaptedInterceptor;
      adaptedInterceptor.setNext(new NextInterceptor());
   }

   public CommandInterceptor getAdaptedInterceptor() {
      return adaptedInterceptor;
   }

   public Class<? extends CommandInterceptor> getAdaptedType() {
      return adaptedInterceptor.getClass();
   }

   @Override
   public CompletableFuture<Object> visitCommand(InvocationContext ctx, VisitableCommand command) {
      AdapterContext actx = ctx.isInTxScope() ? new TxAdapterContext(ctx) : new AdapterContext(ctx);
      actx.adaptedFuture = new CompletableFuture<>();
      executor.execute(() -> {
         try {
            if (trace)
               log.tracef("Executing legacy interceptor %s", adaptedInterceptor);
            Object returnValue = command.acceptVisitor(actx, adaptedInterceptor);
            actx.adaptedFuture.complete(returnValue);
            if (!actx.beforeFuture.isDone()) {
               // The interceptor didn't call command.acceptVisitor(next)
               // We need to run this in a separate thread
               actx.beforeFuture.complete(ctx.shortCircuit(returnValue));
            }
            if (trace)
               log.tracef("Finished legacy interceptor %s", adaptedInterceptor);
         } catch (Throwable throwable) {
            if (trace)
               log.tracef(throwable, "Exception in legacy interceptor %s", adaptedInterceptor);
            actx.adaptedFuture.completeExceptionally(throwable);
            if (!actx.beforeFuture.isDone()) {
               // We need to run this in a separate thread
               actx.beforeFuture.completeExceptionally(throwable);
            }
            throw new CacheException(throwable);
         }
      });
      return actx.beforeFuture;
   }

   @Override
   public String toString() {
      return "SequentialInterceptorAdapter(" + adaptedInterceptor.getClass().getSimpleName() + ')';
   }

   public class NextInterceptor extends CommandInterceptor {
      @Override
      protected Object handleDefault(InvocationContext ctx, VisitableCommand command) throws Throwable {
         AdapterContext actx = (AdapterContext) ctx;
         actx.nextInterceptorFuture = new CompletableFuture<>();
         if (command != actx.sctx.getCommand()) {
            // The interceptor started executing a different command, fork the execution and
            // create a new beforeFuture
            CompletableFuture<Object> savedFuture = actx.beforeFuture;
            actx.beforeFuture = new CompletableFuture<>();
            savedFuture.complete(actx.sctx.forkInvocation(command, (returnValue, throwable) -> {
               if (throwable == null) {
                  actx.nextInterceptorFuture.complete(returnValue);
               } else {
                  actx.nextInterceptorFuture.completeExceptionally(throwable);
               }
               return actx.adaptedFuture;
            }));
         } else {
            // Executing the next interceptor with the same command
            actx.sctx.onReturn((returnValue, throwable) -> {
               if (throwable == null) {
                  actx.nextInterceptorFuture.complete(returnValue);
               } else {
                  actx.nextInterceptorFuture.completeExceptionally(throwable);
               }
               if (trace)
                  log.tracef("Next interceptor done, waiting for current interceptor %s to finish",
                             SequentialInterceptorAdapter.this);
               return actx.adaptedFuture;
            });
            actx.beforeFuture.complete(null);
         }
         return actx.nextInterceptorFuture.get();
      }
   }

   private static class LegacyInterceptorThreadFactory implements ThreadFactory {
      public static final LegacyInterceptorThreadFactory INSTANCE = new LegacyInterceptorThreadFactory();

      private final AtomicInteger counter = new AtomicInteger(0);

      @Override
      public Thread newThread(Runnable r) {
         return new Thread(r, "LegacyInterceptor-" + counter.incrementAndGet());
      }
   }

   static class AdapterContext implements InvocationContext {
      protected final InvocationContext sctx;
      // Done when the command was passed to the next SequentialInterceptor in the chain
      volatile CompletableFuture<Object> beforeFuture = new CompletableFuture<>();
      // Done when the next SequentialInterceptor returns
      volatile CompletableFuture<Object> nextInterceptorFuture = null;
      // Done when the adapted interceptor finished executing
      volatile CompletableFuture<Object> adaptedFuture;

      public AdapterContext(InvocationContext sctx) {
         this.sctx = sctx;
      }

      @Override
      public boolean isOriginLocal() {
         return sctx.isOriginLocal();
      }

      @Override
      public Address getOrigin() {
         return sctx.getOrigin();
      }

      @Override
      public boolean isInTxScope() {
         return sctx.isInTxScope();
      }

      @Override
      public Object getLockOwner() {
         return sctx.getLockOwner();
      }

      @Override
      public void setLockOwner(Object lockOwner) {
         sctx.setLockOwner(lockOwner);
      }

      @Override
      public InvocationContext clone() {
         return sctx.clone();
      }

      @Override
      public Set<Object> getLockedKeys() {
         return sctx.getLockedKeys();
      }

      @Override
      public void clearLockedKeys() {
         sctx.clearLockedKeys();
      }

      @Override
      public ClassLoader getClassLoader() {
         return sctx.getClassLoader();
      }

      @Override
      public void setClassLoader(ClassLoader classLoader) {
         sctx.setClassLoader(classLoader);
      }

      @Override
      public void addLockedKey(Object key) {
         sctx.addLockedKey(key);
      }

      @Override
      public boolean hasLockedKey(Object key) {
         return sctx.hasLockedKey(key);
      }

      @Override
      public boolean replaceValue(Object key, InternalCacheEntry cacheEntry) {
         return sctx.replaceValue(key, cacheEntry);
      }

      @Override
      public boolean isEntryRemovedInContext(Object key) {
         return sctx.isEntryRemovedInContext(key);
      }

      @Override
      public CacheEntry lookupEntry(Object key) {
         return sctx.lookupEntry(key);
      }

      @Override
      public Map<Object, CacheEntry> getLookedUpEntries() {
         return sctx.getLookedUpEntries();
      }

      @Override
      public void putLookedUpEntry(Object key, CacheEntry e) {
         sctx.putLookedUpEntry(key, e);
      }

      @Override
      public void removeLookedUpEntry(Object key) {
         sctx.removeLookedUpEntry(key);
      }

      @Override
      public VisitableCommand getCommand() {
         throw new UnsupportedOperationException();
      }

      @Override
      public void onReturn(BiFunction<Object, Throwable, CompletableFuture<Object>> returnHandler) {
         throw new UnsupportedOperationException();
      }

      @Override
      public CompletableFuture<Object> shortCircuit(Object returnValue) {
         throw new UnsupportedOperationException();
      }

      @Override
      public CompletableFuture<Object> forkInvocation(VisitableCommand newCommand,
                                                      BiFunction<Object, Throwable, CompletableFuture<Object>> returnHandler) {
         throw new UnsupportedOperationException();
      }

      @Override
      public CompletableFuture<Object> execute(VisitableCommand command) {
         throw new UnsupportedOperationException();
      }
   }

   static class TxAdapterContext extends AdapterContext implements TxInvocationContext {
      public TxAdapterContext(InvocationContext sctx) {
         super(sctx);
      }

      @Override
      public boolean hasModifications() {
         return ((TxInvocationContext) sctx).hasModifications();
      }

      @Override
      public Set<Object> getAffectedKeys() {
         return ((TxInvocationContext) sctx).getAffectedKeys();
      }

      @Override
      public GlobalTransaction getGlobalTransaction() {
         return ((TxInvocationContext) sctx).getGlobalTransaction();
      }

      @Override
      public List<WriteCommand> getModifications() {
         return ((TxInvocationContext) sctx).getModifications();
      }

      @Override
      public Transaction getTransaction() {
         return ((TxInvocationContext) sctx).getTransaction();
      }

      @Override
      public void addAffectedKey(Object key) {
         ((TxInvocationContext) sctx).addAffectedKey(key);
      }

      @Override
      public boolean isTransactionValid() {
         return ((TxInvocationContext) sctx).isTransactionValid();
      }

      @Override
      public boolean isImplicitTransaction() {
         return ((TxInvocationContext) sctx).isImplicitTransaction();
      }

      @Override
      public AbstractCacheTransaction getCacheTransaction() {
         return ((TxInvocationContext) sctx).getCacheTransaction();
      }

      @Override
      public void addAllAffectedKeys(Collection keys) {
         ((TxInvocationContext) sctx).addAllAffectedKeys(keys);
      }
   }
}

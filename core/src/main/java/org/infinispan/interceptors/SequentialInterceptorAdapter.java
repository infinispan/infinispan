package org.infinispan.interceptors;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.write.WriteCommand;
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
      executor.execute(() -> asyncVisit(ctx, command, actx));
      return actx.visitFuture;
   }

   protected void asyncVisit(InvocationContext ctx, VisitableCommand command, AdapterContext actx) {
      try {
         if (trace)
            log.tracef("Executing legacy interceptor %s for %s", getAdaptedType().getSimpleName(), command);
         Object returnValue = command.acceptVisitor(actx, adaptedInterceptor);
         if (trace)
            log.tracef("Finished legacy interceptor %s for %s", getAdaptedType().getSimpleName(), command);
         actx.adaptedFuture.complete(returnValue);
         actx.visitFuture.complete(ctx.shortCircuit(returnValue));
      } catch (Throwable throwable) {
         if (trace)
            log.tracef(throwable, "Exception in legacy interceptor %s for %s", getAdaptedType().getSimpleName(), command);
         actx.adaptedFuture.completeExceptionally(throwable);
         actx.visitFuture.completeExceptionally(throwable);
      }
   }

   Object handleNextInterceptor(AdapterContext ctx, VisitableCommand command) throws Throwable {
      AdapterContext actx = ctx;
      CompletableFuture<Object> nextInterceptorFuture = new CompletableFuture<>();
      // The legacy interceptors can start executing a new command even on the return path,
      // but forkInvocation() doesn't work on the return path.
      // So we have to fork even for the same command, and short-circuit when the legacy
      // interceptor is done.
      // TODO If this is common enough, maybe forking should be the default??
      BiFunction<Object, Throwable, CompletableFuture<Object>> returnHandler =
            (returnValue, throwable) -> handleNextReturn(actx, nextInterceptorFuture, returnValue, throwable);
      if (trace) log.tracef("visitFuture done for %s", getAdaptedType().getSimpleName());
      actx.visitFuture.complete(actx.sctx.forkInvocation(command, returnHandler));
      try {
         if (trace) log.trace("Waiting for next interceptor's return handler");
         return nextInterceptorFuture.get();
      } catch (ExecutionException e) {
         Throwable cause = e.getCause();
         throw cause;
      }
   }

   private CompletableFuture<Object> handleNextReturn(AdapterContext actx,
                                                      CompletableFuture<Object> nextInterceptorFuture,
                                                      Object returnValue, Throwable throwable) {
      if (trace)
         log.tracef("Resuming legacy interceptor %s for %s", getAdaptedType().getSimpleName(), actx.sctx.getCommand());
      actx.visitFuture = new CompletableFuture<>();
      if (throwable == null) {
         nextInterceptorFuture.complete(returnValue);
      } else {
         nextInterceptorFuture.completeExceptionally(throwable);
      }
      // May be already done by now...
      return actx.visitFuture;
   }

   @Override
   public String toString() {
      return "SequentialInterceptorAdapter(" + getAdaptedType().getSimpleName() + ')';
   }

   public class NextInterceptor extends CommandInterceptor {

      @Override
      protected Object handleDefault(InvocationContext ctx, VisitableCommand command) throws Throwable {
         return handleNextInterceptor((AdapterContext) ctx, command);
      }

      @Override
      public String toString() {
         return "NextInterceptor(" + getAdaptedType().getSimpleName() + ")";
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

   class AdapterContext implements InvocationContext {
      protected final InvocationContext sctx;
      // Done when the command was passed to the next SequentialInterceptor in the chain
      volatile CompletableFuture<Object> visitFuture = new CompletableFuture<>();
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

      @Override
      public String toString() {
         return "AdapterContext(" + getAdaptedType().getSimpleName() + ")";
      }
   }

   class TxAdapterContext<T extends AbstractCacheTransaction> extends AdapterContext
         implements TxInvocationContext<T> {
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
      public T getCacheTransaction() {
         return ((TxInvocationContext<T>) sctx).getCacheTransaction();
      }

      @Override
      public void addAllAffectedKeys(Collection keys) {
         ((TxInvocationContext) sctx).addAllAffectedKeys(keys);
      }
   }
}

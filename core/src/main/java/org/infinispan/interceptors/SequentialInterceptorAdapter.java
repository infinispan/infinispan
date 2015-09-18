package org.infinispan.interceptors;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.commons.CacheException;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.interceptors.base.BaseSequentialInterceptor;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.remoting.transport.Address;

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
   private final CommandInterceptor adaptedInterceptor;

   // TODO Inject
   final ExecutorService executor = Executors.newCachedThreadPool(new ThreadFactory() {
      final AtomicInteger counter = new AtomicInteger(0);

      @Override
      public Thread newThread(Runnable r) {
         return new Thread(r, "LegacyInterceptor-" + counter.incrementAndGet());
      }
   });

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
      AdapterContext actx = new AdapterContext(ctx);
      actx.adaptedFuture = CompletableFuture.supplyAsync(() -> {
         try {
            Object returnValue = command.acceptVisitor(actx, adaptedInterceptor);
            actx.beforeFuture.complete(ctx.shortCircuit(returnValue));
            return returnValue;
         } catch (Throwable throwable) {
            throw new CacheException(throwable);
         }
      }, executor);
      return actx.beforeFuture;
   }

   public static class NextInterceptor extends CommandInterceptor {
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
               try {
                  return actx.beforeFuture.get();
               } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                  throw new CacheException(e);
               } catch (ExecutionException e) {
                  throw new CacheException(e.getCause());
               }
            }));
         } else {
            // Executing the next interceptor with the same command
            actx.sctx.onReturn((returnValue, throwable) -> {
               if (throwable == null) {
                  actx.nextInterceptorFuture.complete(returnValue);
               } else {
                  actx.nextInterceptorFuture.completeExceptionally(throwable);
               }
               return actx.adaptedFuture;
            });
         }
         return actx.nextInterceptorFuture.get();
      }
   }

   private class AdapterContext implements InvocationContext {
      private final InvocationContext sctx;
      // Done when the command was passed to the next SequentialInterceptor in the chain
      CompletableFuture<Object> beforeFuture = new CompletableFuture<>();
      // Done when the next SequentialInterceptor returns
      CompletableFuture<Object> nextInterceptorFuture = null;
      // Done when the adapted interceptor finished executing
      public CompletableFuture<Object> adaptedFuture;

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
      public void onReturn(BiFunction<Object, Throwable, Object> returnHandler) {
         throw new UnsupportedOperationException();
      }

      @Override
      public CompletableFuture<Object> shortCircuit(Object returnValue) {
         throw new UnsupportedOperationException();
      }

      @Override
      public CompletableFuture<Object> forkInvocation(VisitableCommand newCommand,
                                                      BiFunction<Object, Throwable, Object> returnHandler) {
         throw new UnsupportedOperationException();
      }

      @Override
      public CompletableFuture<Object> execute(VisitableCommand command) {
         throw new UnsupportedOperationException();
      }
   }
}

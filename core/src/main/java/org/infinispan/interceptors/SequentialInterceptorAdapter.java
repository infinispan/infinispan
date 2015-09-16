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
import java.util.function.UnaryOperator;

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

   public Class<? extends CommandInterceptor> getAdaptedType() {
      return adaptedInterceptor.getClass();
   }

   @Override
   public BiFunction<Object, Throwable, Object> visitCommand(InvocationContext ctx, VisitableCommand command) {
      AdapterContext actx = new AdapterContext(ctx);
      CompletableFuture<Object> cf = CompletableFuture.supplyAsync(() -> {
         try {
            Object returnValue = command.acceptVisitor(actx, adaptedInterceptor);
            if (!actx.beforeFuture.isDone()) {
               actx.beforeFuture.complete(skipNextInterceptor(returnValue));
            }
            return returnValue;
         } catch (Throwable throwable) {
            throw new CacheException(throwable);
         }
      }, executor);
      actx.beforeFuture.join();
      return returnValue -> afterCommand(actx, cf, returnValue);
   }

   protected Object afterCommand(AdapterContext actx, CompletableFuture<Object> cf, Object returnValue) {
      try {
         actx.nextInterceptorFuture.complete(returnValue);
         return cf.get();
      } catch (InterruptedException e) {
         Thread.currentThread().interrupt();
         throw new CacheException(e);
      } catch (ExecutionException e) {
         throw new CacheException(e.getCause());
      }
   }

   public static class NextInterceptor extends CommandInterceptor {
      @Override
      protected Object handleDefault(InvocationContext ctx, VisitableCommand command) throws Throwable {
         AdapterContext actx = (AdapterContext) ctx;
         return actx.nextInterceptorFuture.get();
      }
   }

   private class AdapterContext implements InvocationContext {
      private final InvocationContext ctx;
      CompletableFuture<BiFunction<Object, Throwable, Object>> beforeFuture = new CompletableFuture<>();
      CompletableFuture<Object> nextInterceptorFuture = new CompletableFuture<>();

      public AdapterContext(InvocationContext ctx) {
         this.ctx = ctx;
      }

      @Override
      public boolean isOriginLocal() {
         return ctx.isOriginLocal();
      }

      @Override
      public Address getOrigin() {
         return ctx.getOrigin();
      }

      @Override
      public boolean isInTxScope() {
         return ctx.isInTxScope();
      }

      @Override
      public Object getLockOwner() {
         return ctx.getLockOwner();
      }

      @Override
      public void setLockOwner(Object lockOwner) {
         ctx.setLockOwner(lockOwner);
      }

      @Override
      public InvocationContext clone() {
         return ctx.clone();
      }

      @Override
      public Set<Object> getLockedKeys() {
         return ctx.getLockedKeys();
      }

      @Override
      public void clearLockedKeys() {
         ctx.clearLockedKeys();
      }

      @Override
      public ClassLoader getClassLoader() {
         return ctx.getClassLoader();
      }

      @Override
      public void setClassLoader(ClassLoader classLoader) {
         ctx.setClassLoader(classLoader);
      }

      @Override
      public void addLockedKey(Object key) {
         ctx.addLockedKey(key);
      }

      @Override
      public boolean hasLockedKey(Object key) {
         return ctx.hasLockedKey(key);
      }

      @Override
      public boolean replaceValue(Object key, InternalCacheEntry cacheEntry) {
         return ctx.replaceValue(key, cacheEntry);
      }

      @Override
      public boolean isEntryRemovedInContext(Object key) {
         return ctx.isEntryRemovedInContext(key);
      }

      @Override
      public CacheEntry lookupEntry(Object key) {
         return ctx.lookupEntry(key);
      }

      @Override
      public Map<Object, CacheEntry> getLookedUpEntries() {
         return ctx.getLookedUpEntries();
      }

      @Override
      public void putLookedUpEntry(Object key, CacheEntry e) {
         ctx.putLookedUpEntry(key, e);
      }

      @Override
      public void removeLookedUpEntry(Object key) {
         ctx.removeLookedUpEntry(key);
      }
   }
}

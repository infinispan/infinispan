package org.infinispan.interceptors.impl;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.control.LockControlCommand;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commons.equivalence.AnyEquivalence;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.SingleKeyNonTxInvocationContext;
import org.infinispan.factories.components.ComponentMetadataRepo;
import org.infinispan.interceptors.AsyncInterceptor;
import org.infinispan.interceptors.AsyncInterceptorChain;
import org.infinispan.interceptors.BaseAsyncInterceptor;
import org.infinispan.interceptors.BasicInvocationStage;
import org.infinispan.interceptors.InterceptorChainTest;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.Exceptions;
import org.infinispan.test.TestException;
import org.testng.annotations.Test;

/**
 * @author Dan Berindei
 * @since 9.0
 */
@Test(groups = "unit", testName = "interceptors.AsyncInterceptorChainInvocationTest")
public class AsyncInterceptorChainInvocationTest extends AbstractInfinispanTest {
   private VisitableCommand testCommand = new GetKeyValueCommand("k", 0);
   private VisitableCommand testSubCommand = new LockControlCommand("k", null, 0, null);

   public void testReturnWith() {
      AsyncInterceptorChain chain = newInterceptorChain(new BaseAsyncInterceptor() {
         @Override
         public BasicInvocationStage visitCommand(InvocationContext ctx, VisitableCommand command) throws Throwable {
            return returnWith("v1");
         }
      }, new BaseAsyncInterceptor() {
         @Override
         public BasicInvocationStage visitCommand(InvocationContext ctx, VisitableCommand command) throws Throwable {
            return returnWith("v2");
         }
      });
      InvocationContext context = newInvocationContext();

      Object returnValue = chain.invoke(context, testCommand);
      assertEquals("v1", returnValue);
   }

   public void testReturnWithAsync() throws Exception {
      CompletableFuture<Object> f = new CompletableFuture<>();
      AsyncInterceptorChain chain = newInterceptorChain(new BaseAsyncInterceptor() {
         @Override
         public BasicInvocationStage visitCommand(InvocationContext ctx, VisitableCommand command) throws Throwable {
            return returnWithAsync(f);
         }
      });
      InvocationContext context = newInvocationContext();

      CompletableFuture<Object> invokeFuture = chain.invokeAsync(context, testCommand);
      assertFalse(invokeFuture.isDone());

      f.complete("v1");
      assertEquals("v1", invokeFuture.get(10, SECONDS));
   }

   public void testSyncCompose() {
      AsyncInterceptorChain chain = newInterceptorChain(new BaseAsyncInterceptor() {
         @Override
         public BasicInvocationStage visitCommand(InvocationContext ctx, VisitableCommand command) throws Throwable {
            return invokeNext(ctx, command).compose((stage, rCtx, rCommand, rv, t) -> returnWith("v1"));
         }
      }, new BaseAsyncInterceptor() {
         @Override
         public BasicInvocationStage visitCommand(InvocationContext ctx, VisitableCommand command) throws Throwable {
            return returnWith("v2");
         }
      });
      InvocationContext context = newInvocationContext();

      Object returnValue = chain.invoke(context, testCommand);
      assertEquals("v1", returnValue);
   }

   public void testAsyncComposeHandler() throws Exception {
      CompletableFuture<Object> f = new CompletableFuture<>();
      AsyncInterceptorChain chain = newInterceptorChain(new BaseAsyncInterceptor() {
         @Override
         public BasicInvocationStage visitCommand(InvocationContext ctx, VisitableCommand command) throws Throwable {
            return invokeNext(ctx, command).compose(
                  (stage, rCtx, rCommand, rv, t) -> returnWithAsync(f));
         }
      }, new BaseAsyncInterceptor() {
         @Override
         public BasicInvocationStage visitCommand(InvocationContext ctx, VisitableCommand command) throws Throwable {
            return returnWith("v1");
         }
      });
      InvocationContext context = newInvocationContext();

      CompletableFuture<Object> invokeFuture = chain.invokeAsync(context, testCommand);
      assertFalse(invokeFuture.isDone());

      f.complete("v2");
      assertEquals("v2", invokeFuture.get(10, SECONDS));
   }

   public void testGoAsync() throws Exception {
      CompletableFuture<Object> f = new CompletableFuture<>();
      AsyncInterceptorChain chain = newInterceptorChain(new BaseAsyncInterceptor() {
         @Override
         public BasicInvocationStage visitCommand(InvocationContext ctx, VisitableCommand command) throws Throwable {
            return goAsync(f.thenApply(this::returnWith));
         }
      });
      InvocationContext context = newInvocationContext();

      CompletableFuture<Object> invokeFuture = chain.invokeAsync(context, testCommand);
      assertFalse(invokeFuture.isDone());

      f.complete("v");
      assertEquals("v", invokeFuture.get(10, SECONDS));
   }

   public void testInvokeNextAsync() throws Exception {
      CompletableFuture<Object> f = new CompletableFuture<>();
      AsyncInterceptorChain chain = newInterceptorChain(new BaseAsyncInterceptor() {
         @Override
         public BasicInvocationStage visitCommand(InvocationContext ctx, VisitableCommand command) throws Throwable {
            return invokeNextAsync(ctx, command, f).thenApply((rCtx, rCommand, rv) -> "v1" + rv);
         }
      }, new BaseAsyncInterceptor() {
         @Override
         public BasicInvocationStage visitCommand(InvocationContext ctx, VisitableCommand command) throws Throwable {
            return returnWith("v2");
         }
      });
      InvocationContext context = newInvocationContext();

      CompletableFuture<Object> invokeFuture = chain.invokeAsync(context, testCommand);
      assertFalse(invokeFuture.isDone());

      f.complete("v");
      assertEquals("v1v2", invokeFuture.get(10, SECONDS));
   }

   public void testInvokeNextSubCommand() {
      AsyncInterceptorChain chain = newInterceptorChain(new BaseAsyncInterceptor() {
         @Override
         public BasicInvocationStage visitCommand(InvocationContext ctx, VisitableCommand command) throws Throwable {
            return invokeNext(ctx, testSubCommand);
         }
      }, new BaseAsyncInterceptor() {
         @Override
         public BasicInvocationStage visitCommand(InvocationContext ctx, VisitableCommand command) throws Throwable {
            return returnWith(command instanceof LockControlCommand ? "subCommand" : "command");
         }
      });
      InvocationContext context = newInvocationContext();

      Object returnValue = chain.invoke(context, testCommand);
      assertEquals("subCommand", returnValue);
   }

   public void testInvokeNextAsyncSubCommand() throws Exception {
      CompletableFuture<Object> f = new CompletableFuture<>();
      AsyncInterceptorChain chain = newInterceptorChain(new BaseAsyncInterceptor() {
         @Override
         public BasicInvocationStage visitCommand(InvocationContext ctx, VisitableCommand command) throws Throwable {
            return invokeNextAsync(ctx, testSubCommand, f);
         }
      }, new BaseAsyncInterceptor() {
         @Override
         public BasicInvocationStage visitCommand(InvocationContext ctx, VisitableCommand command) throws Throwable {
            return returnWith(command instanceof LockControlCommand ? "subCommand" : "command");
         }
      });
      InvocationContext context = newInvocationContext();

      CompletableFuture<Object> invokeFuture = chain.invokeAsync(context, testCommand);
      assertFalse(invokeFuture.isDone());
      f.complete("v");
      assertEquals("subCommand", invokeFuture.get(10, SECONDS));
   }

   public void testReturnWithAsyncCompose() throws Exception {
      CompletableFuture<Object> f = new CompletableFuture<>();
      AsyncInterceptorChain chain = newInterceptorChain(new BaseAsyncInterceptor() {
         @Override
         public BasicInvocationStage visitCommand(InvocationContext ctx, VisitableCommand command) throws Throwable {
            return invokeNext(ctx, command).compose((stage, rCtx, rCommand, rv, t) -> returnWith("v1"));
         }
      }, new BaseAsyncInterceptor() {
         @Override
         public BasicInvocationStage visitCommand(InvocationContext ctx, VisitableCommand command) throws Throwable {
            return returnWithAsync(f);
         }
      });
      InvocationContext context = newInvocationContext();

      CompletableFuture<Object> invokeFuture = chain.invokeAsync(context, testCommand);
      assertFalse(invokeFuture.isDone());
      f.complete("v2");
      assertEquals("v1", invokeFuture.get(10, SECONDS));
   }

   public void testSyncException() throws Exception {
      testException(new ExceptionTestCallbacks() {

         @Override
         public BasicInvocationStage perform(BaseAsyncInterceptor interceptor) {
            throw new TestException("bla");
         }

         @Override
         public void post(CompletableFuture<Object> invokeFuture) {
            assertTrue(invokeFuture.isDone());
         }
      });
   }

   public void testAsyncException() throws Exception {
      CompletableFuture<Object> f = new CompletableFuture<>();
      testException(new ExceptionTestCallbacks() {

         @Override
         public BasicInvocationStage perform(BaseAsyncInterceptor interceptor) {
            return interceptor.returnWithAsync(f);
         }

         @Override
         public void post(CompletableFuture<Object> invokeFuture) {
            f.completeExceptionally(new TestException("bla"));
         }
      });
   }

   public void testComposeAsyncException() throws Exception {
      CompletableFuture<BasicInvocationStage> f = new CompletableFuture<>();
      testException(new ExceptionTestCallbacks() {

         @Override
         public BasicInvocationStage perform(BaseAsyncInterceptor interceptor) {
            return interceptor.goAsync(f);
         }

         @Override
         public void post(CompletableFuture<Object> invokeFuture) {
            f.completeExceptionally(new TestException("bla"));
         }
      });
   }

   private void testException(ExceptionTestCallbacks callbacks) throws Exception {
      AtomicBoolean compose = new AtomicBoolean();
      AtomicBoolean handle = new AtomicBoolean();
      AtomicBoolean exceptionally = new AtomicBoolean();
      AtomicBoolean thenAccept = new AtomicBoolean();
      AtomicBoolean thenApply = new AtomicBoolean();
      AsyncInterceptorChain chain = newInterceptorChain(new BaseAsyncInterceptor() {
         @Override
         public BasicInvocationStage visitCommand(InvocationContext ctx, VisitableCommand command) throws Throwable {
            return invokeNext(ctx, command).compose((stage, rCtx, rCommand, rv, t) -> {
               compose.set(true);
               return stage;
            });
         }
      }, new BaseAsyncInterceptor() {
         @Override
         public BasicInvocationStage visitCommand(InvocationContext ctx, VisitableCommand command) throws Throwable {
            return invokeNext(ctx, command).handle((rCtx, rCommand, rv, t) -> {
               handle.set(true);
            });
         }
      }, new BaseAsyncInterceptor() {
         @Override
         public BasicInvocationStage visitCommand(InvocationContext ctx, VisitableCommand command) throws Throwable {
            return invokeNext(ctx, command).exceptionally((rCtx, rCommand, t) -> {
               exceptionally.set(true);
               throw t;
            });
         }
      }, new BaseAsyncInterceptor() {
         @Override
         public BasicInvocationStage visitCommand(InvocationContext ctx, VisitableCommand command) throws Throwable {
            return invokeNext(ctx, command).thenAccept((rCtx, rCommand, rv) -> {
               thenAccept.set(true);
            });
         }
      }, new BaseAsyncInterceptor() {
         @Override
         public BasicInvocationStage visitCommand(InvocationContext ctx, VisitableCommand command) throws Throwable {
            return invokeNext(ctx, command).thenApply((rCtx, rCommand, rv) -> {
               thenApply.set(true);
               return rv;
            });
         }
      }, new BaseAsyncInterceptor() {
         @Override
         public BasicInvocationStage visitCommand(InvocationContext ctx, VisitableCommand command) throws Throwable {
            return callbacks.perform(this);
         }
      });
      InvocationContext context = newInvocationContext();

      CompletableFuture<Object> invokeFuture = chain.invokeAsync(context, testCommand);
      callbacks.post(invokeFuture);
      Exceptions.expectExecutionException(TestException.class, invokeFuture);
      assertTrue(compose.get());
      assertTrue(handle.get());
      assertTrue(exceptionally.get());
      assertFalse(thenAccept.get());
      assertFalse(thenApply.get());
   }

   protected SingleKeyNonTxInvocationContext newInvocationContext() {
      // Actual implementation doesn't matter, we are only testing the BaseAsyncInvocationContext methods
      return new SingleKeyNonTxInvocationContext(null, AnyEquivalence.getInstance());
   }

   protected AsyncInterceptorChain newInterceptorChain(AsyncInterceptor... interceptors) {
      ComponentMetadataRepo componentMetadataRepo = new ComponentMetadataRepo();
      componentMetadataRepo.initialize(Collections.emptyList(), InterceptorChainTest.class.getClassLoader());

      AsyncInterceptorChain chain = new AsyncInterceptorChainImpl(componentMetadataRepo);
      for (AsyncInterceptor i : interceptors) {
         chain.appendInterceptor(i, false);
      }
      return chain;
   }

   private interface ExceptionTestCallbacks {
      BasicInvocationStage perform(BaseAsyncInterceptor interceptor);
      void post(CompletableFuture<Object> invokeFuture);
   }
}

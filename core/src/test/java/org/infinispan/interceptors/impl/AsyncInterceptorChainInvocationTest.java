package org.infinispan.interceptors.impl;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.infinispan.test.Exceptions.expectExecutionException;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.control.LockControlCommand;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.SingleKeyNonTxInvocationContext;
import org.infinispan.factories.components.ComponentMetadataRepo;
import org.infinispan.interceptors.AsyncInterceptor;
import org.infinispan.interceptors.AsyncInterceptorChain;
import org.infinispan.interceptors.BaseAsyncInterceptor;
import org.infinispan.interceptors.InterceptorChainTest;
import org.infinispan.interceptors.InvocationSuccessFunction;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestException;
import org.infinispan.util.concurrent.CompletableFutures;
import org.testng.annotations.Test;

/**
 * @author Dan Berindei
 * @since 9.0
 */
@Test(groups = "unit", testName = "interceptors.AsyncInterceptorChainInvocationTest")
public class AsyncInterceptorChainInvocationTest extends AbstractInfinispanTest {
   private VisitableCommand testCommand = new GetKeyValueCommand("k", 0, 0);
   private VisitableCommand testSubCommand = new LockControlCommand("k", null, 0, null);

   private final AtomicReference<String> sideEffects = new AtomicReference<>("");

   public void testCompletedStage() {
      AsyncInterceptorChain chain = newInterceptorChain(new BaseAsyncInterceptor() {
         @Override
         public Object visitCommand(InvocationContext ctx, VisitableCommand command) throws Throwable {
            return "v1";
         }
      }, new BaseAsyncInterceptor() {
         @Override
         public Object visitCommand(InvocationContext ctx, VisitableCommand command) throws Throwable {
            return "v2";
         }
      });
      InvocationContext context = newInvocationContext();

      Object returnValue = chain.invoke(context, testCommand);
      assertEquals("v1", returnValue);
   }

   public void testAsyncStage() throws Exception {
      CompletableFuture<Object> f = new CompletableFuture<>();
      AsyncInterceptorChain chain = newInterceptorChain(new BaseAsyncInterceptor() {
         @Override
         public Object visitCommand(InvocationContext ctx, VisitableCommand command) throws Throwable {
            return asyncValue(f);
         }
      });
      InvocationContext context = newInvocationContext();

      CompletableFuture<Object> invokeFuture = chain.invokeAsync(context, testCommand);
      assertFalse(invokeFuture.isDone());

      f.complete("v1");
      assertEquals("v1", invokeFuture.get(10, SECONDS));
   }

   public void testComposeSync() {
      AsyncInterceptorChain chain = newInterceptorChain(new BaseAsyncInterceptor() {
         @Override
         public Object visitCommand(InvocationContext ctx, VisitableCommand command) throws Throwable {
            return invokeNextAndHandle(ctx, command, (rCtx, rCommand, rv, t) -> "v1");
         }
      }, new BaseAsyncInterceptor() {
         @Override
         public Object visitCommand(InvocationContext ctx, VisitableCommand command) throws Throwable {
            return "v2";
         }
      });
      InvocationContext context = newInvocationContext();

      Object returnValue = chain.invoke(context, testCommand);
      assertEquals("v1", returnValue);
   }

   public void testComposeAsync() throws Exception {
      CompletableFuture<Object> f = new CompletableFuture<>();
      AsyncInterceptorChain chain = newInterceptorChain(new BaseAsyncInterceptor() {
         @Override
         public Object visitCommand(InvocationContext ctx, VisitableCommand command) throws Throwable {
            return invokeNextAndHandle(ctx, command, (rCtx, rCommand, rv, t) -> asyncValue(f));
         }
      }, new BaseAsyncInterceptor() {
         @Override
         public Object visitCommand(InvocationContext ctx, VisitableCommand command) throws Throwable {
            return "v1";
         }
      });
      InvocationContext context = newInvocationContext();

      CompletableFuture<Object> invokeFuture = chain.invokeAsync(context, testCommand);
      assertFalse(invokeFuture.isDone());

      f.complete("v2");
      assertEquals("v2", invokeFuture.get(10, SECONDS));
   }

   public void testInvokeNextAsync() throws Exception {
      CompletableFuture<Object> f = new CompletableFuture<>();
      AsyncInterceptorChain chain = newInterceptorChain(new BaseAsyncInterceptor() {
         @Override
         public Object visitCommand(InvocationContext ctx, VisitableCommand command) throws Throwable {
            return asyncInvokeNext(ctx, command, f);
         }
      }, new BaseAsyncInterceptor() {
         @Override
         public Object visitCommand(InvocationContext ctx, VisitableCommand command) throws Throwable {
            return "v1";
         }
      });
      InvocationContext context = newInvocationContext();

      CompletableFuture<Object> invokeFuture = chain.invokeAsync(context, testCommand);
      assertFalse(invokeFuture.isDone());

      f.complete("v");
      assertEquals("v1", invokeFuture.get(10, SECONDS));
   }

   public void testInvokeNextSubCommand() {
      AsyncInterceptorChain chain = newInterceptorChain(new BaseAsyncInterceptor() {
         @Override
         public Object visitCommand(InvocationContext ctx, VisitableCommand command) throws Throwable {
            return invokeNext(ctx, testSubCommand);
         }
      }, new BaseAsyncInterceptor() {
         @Override
         public Object visitCommand(InvocationContext ctx, VisitableCommand command) throws Throwable {
            return command instanceof LockControlCommand ? "subCommand" : "command";
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
         public Object visitCommand(InvocationContext ctx, VisitableCommand command) throws Throwable {
            return asyncInvokeNext(ctx, testSubCommand, f);
         }
      }, new BaseAsyncInterceptor() {
         @Override
         public Object visitCommand(InvocationContext ctx, VisitableCommand command) throws Throwable {
            return command instanceof LockControlCommand ? "subCommand" : "command";
         }
      });
      InvocationContext context = newInvocationContext();

      CompletableFuture<Object> invokeFuture = chain.invokeAsync(context, testCommand);
      assertFalse(invokeFuture.isDone());
      f.complete("v");
      assertEquals("subCommand", invokeFuture.get(10, SECONDS));
   }

   public void testAsyncStageCompose() throws Exception {
      CompletableFuture<Object> f = new CompletableFuture<>();
      AsyncInterceptorChain chain = newInterceptorChain(new BaseAsyncInterceptor() {
         @Override
         public Object visitCommand(InvocationContext ctx, VisitableCommand command) throws Throwable {
            return invokeNextAndHandle(ctx, command, (rCtx, rCommand, rv, t) -> "v1");
         }
      }, new BaseAsyncInterceptor() {
         @Override
         public Object visitCommand(InvocationContext ctx, VisitableCommand command) throws Throwable {
            return asyncValue(f);
         }
      });
      InvocationContext context = newInvocationContext();

      CompletableFuture<Object> invokeFuture = chain.invokeAsync(context, testCommand);
      assertFalse(invokeFuture.isDone());
      f.complete("v2");
      assertEquals("v1", invokeFuture.get(10, SECONDS));
   }

   public void testAsyncStageComposeAsyncStage() throws Exception {
      CompletableFuture<Object> f1 = new CompletableFuture<>();
      CompletableFuture<Object> f2 = new CompletableFuture<>();
      CompletableFuture<Object> f3 = new CompletableFuture<>();
      AsyncInterceptorChain chain = newInterceptorChain(new BaseAsyncInterceptor() {
         @Override
         public Object visitCommand(InvocationContext ctx, VisitableCommand command) throws Throwable {
            return invokeNextAndHandle(ctx, command, (rCtx, rCommand, rv, t) -> {
               InvocationSuccessFunction function = (rCtx1, rCommand1, rv1) -> asyncValue(f3);
               return asyncValue(f2).addCallback(rCtx, rCommand, function);
            });
         }
      }, new BaseAsyncInterceptor() {
         @Override
         public Object visitCommand(InvocationContext ctx, VisitableCommand command) throws Throwable {
            return asyncValue(f1);
         }
      });
      InvocationContext context = newInvocationContext();

      CompletableFuture<Object> invokeFuture = chain.invokeAsync(context, testCommand);
      assertFalse(invokeFuture.isDone());
      f1.complete("v1");
      assertFalse(invokeFuture.isDone());
      f2.complete("v2");
      assertFalse(invokeFuture.isDone());
      f3.complete("v3");
      assertEquals("v3", invokeFuture.get(10, SECONDS));
   }

   public void testAsyncInvocationManyHandlers() throws Exception {
      sideEffects.set("");
      CompletableFuture<Object> f = new CompletableFuture<>();
      AsyncInterceptorChain chain = makeChainWithManyHandlers(f);

      CompletableFuture<Object> invokeFuture = chain.invokeAsync(newInvocationContext(), testCommand);
      f.complete("");

      assertHandlers(invokeFuture);
   }

   public void testSyncInvocationManyHandlers() throws Exception {
      sideEffects.set("");
      CompletableFuture<Object> f = CompletableFuture.completedFuture("");
      AsyncInterceptorChain chain = makeChainWithManyHandlers(f);

      CompletableFuture<Object> invokeFuture = chain.invokeAsync(newInvocationContext(), testCommand);

      assertHandlers(invokeFuture);
   }

   private void assertHandlers(CompletableFuture<Object> invokeFuture)
         throws InterruptedException, ExecutionException {
      assertEquals("|handle|thenApply", invokeFuture.get());
      assertEquals("|whenComplete|handle|thenAccept|thenApply", sideEffects.get());
   }

   public void testAsyncInvocationManyHandlersSyncException() throws Exception {
      sideEffects.set("");
      CompletableFuture<Object> f = CompletableFutures.completedExceptionFuture(new TestException(""));
      AsyncInterceptorChain chain = makeChainWithManyHandlers(f);
      CompletableFuture<Object> invokeFuture = chain.invokeAsync(newInvocationContext(), testCommand);
      assertExceptionHandlers(invokeFuture);
   }

   public void testAsyncInvocationManyHandlersAsyncException() throws Exception {
      sideEffects.set("");
      CompletableFuture<Object> f = new CompletableFuture<>();
      AsyncInterceptorChain chain = makeChainWithManyHandlers(f);
      CompletableFuture<Object> invokeFuture = chain.invokeAsync(newInvocationContext(), testCommand);
      f.completeExceptionally(new TestException(""));
      assertExceptionHandlers(invokeFuture);
   }

   private void assertExceptionHandlers(CompletableFuture<Object> invokeFuture) {
      String expectedMessage = "|whenComplete|handle|exceptionally";
      expectExecutionException(TestException.class, Pattern.quote(expectedMessage), invokeFuture);
      assertEquals("|whenComplete|handle|exceptionally", sideEffects.get());
   }

   private AsyncInterceptorChain makeChainWithManyHandlers(CompletableFuture<Object> f) {
      return newInterceptorChain(new BaseAsyncInterceptor() {
         @Override
         public Object visitCommand(InvocationContext ctx, VisitableCommand command) throws Throwable {
            return invokeNextThenApply(ctx, command, (rCtx, rCommand, rv) ->
                  afterInvokeNext(ctx, rCtx, command, rCommand, rv, null, "|thenApply"));
         }
      }, new BaseAsyncInterceptor() {
         @Override
         public Object visitCommand(InvocationContext ctx, VisitableCommand command) throws Throwable {
            return invokeNextThenAccept(ctx, command, (rCtx, rCommand, rv) ->
                  afterInvokeNext(ctx, rCtx, command, rCommand, rv, null, "|thenAccept"));
         }
      }, new BaseAsyncInterceptor() {
         @Override
         public Object visitCommand(InvocationContext ctx, VisitableCommand command) throws Throwable {
            return invokeNextAndExceptionally(ctx, command, (rCtx, rCommand, t) ->
                  afterInvokeNext(ctx, rCtx, command, rCommand, null, t, "|exceptionally"));
         }
      }, new BaseAsyncInterceptor() {
         @Override
         public Object visitCommand(InvocationContext ctx, VisitableCommand command) throws Throwable {
            return invokeNextAndHandle(ctx, command, (rCtx, rCommand, rv, t) ->
                  afterInvokeNext(ctx, rCtx, command, rCommand, rv, t, "|handle"));
         }
      }, new BaseAsyncInterceptor() {
         @Override
         public Object visitCommand(InvocationContext ctx, VisitableCommand command) throws Throwable {
            return invokeNextAndFinally(ctx, command, (rCtx, rCommand, rv, t) ->
                  afterInvokeNext(ctx, rCtx, command, rCommand, rv, t, "|whenComplete"));
         }
      }, new BaseAsyncInterceptor() {
         @Override
         public Object visitCommand(InvocationContext ctx, VisitableCommand command) throws Throwable {
            return asyncValue(f);
         }
      });
   }

   private String afterInvokeNext(Object rv, Throwable t, String text) {
      sideEffects.set(sideEffects.get() + text);
      if (t == null) {
         return rv.toString() + text;
      } else {
         throw new TestException(t.getMessage() + text);
      }
   }

   private String afterInvokeNext(VisitableCommand expectedCommand, VisitableCommand command, Object rv, Throwable t,
                                  String text) {
      assertEquals(expectedCommand, command);
      return afterInvokeNext(rv, t, text);
   }

   private String afterInvokeNext(InvocationContext expectedCtx, InvocationContext ctx,
                                  VisitableCommand expectedCommand, VisitableCommand command, Object rv, Throwable t,
                                  String text) {
      assertEquals(expectedCtx, ctx);
      return afterInvokeNext(expectedCommand, command, rv, t, text);
   }

   public void testDeadlockWithAsyncStage() throws Exception {
      CompletableFuture<Object> f1 = new CompletableFuture<>();
      CompletableFuture<Object> f2 = new CompletableFuture<>();
      AsyncInterceptorChain chain = newInterceptorChain(new BaseAsyncInterceptor() {
         @Override
         public Object visitCommand(InvocationContext ctx, VisitableCommand command) throws Throwable {
            return invokeNextThenApply(ctx, command, (rCtx, rCommand, rv) -> rv + " " + awaitFuture(f2));
         }
      }, new BaseAsyncInterceptor() {
         @Override
         public Object visitCommand(InvocationContext ctx, VisitableCommand command) throws Throwable {
            // Add a handler to force the return value to be a full AsyncInvocationStage
            InvocationSuccessFunction function = (rCtx, rCommand, rv) -> rv;
            return asyncValue(f1).addCallback(ctx, command, function);
         }
      });
      InvocationContext context = newInvocationContext();

      CompletableFuture<Object> invokeFuture = chain.invokeAsync(context, testCommand);
      assertFalse(invokeFuture.isDone());
      Future<Boolean> fork = fork(() -> f1.complete("v1"));
      Thread.sleep(100);
      assertFalse(fork.isDone());
      assertFalse(invokeFuture.isDone());
      f2.complete("v2");
      fork.get(10, SECONDS);
      assertEquals("v1 v2", invokeFuture.getNow(null));
   }

   private Object awaitFuture(CompletableFuture<Object> f2) {
      try {
         return f2.get(10, SECONDS);
      } catch (InterruptedException | ExecutionException | TimeoutException e) {
         throw CompletableFutures.asCompletionException(e);
      }
   }


   private SingleKeyNonTxInvocationContext newInvocationContext() {
      // Actual implementation doesn't matter, we are only testing the BaseAsyncInvocationContext methods
      return new SingleKeyNonTxInvocationContext(null);
   }

   private AsyncInterceptorChain newInterceptorChain(AsyncInterceptor... interceptors) {
      ComponentMetadataRepo componentMetadataRepo = new ComponentMetadataRepo();
      componentMetadataRepo.initialize(Collections.emptyList(), InterceptorChainTest.class.getClassLoader());

      AsyncInterceptorChain chain = new AsyncInterceptorChainImpl(componentMetadataRepo);
      for (AsyncInterceptor i : interceptors) {
         chain.appendInterceptor(i, false);
      }
      return chain;
   }
}

package org.infinispan.context.impl;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.control.LockControlCommand;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commons.equivalence.AnyEquivalence;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.SingleKeyNonTxInvocationContext;
import org.infinispan.factories.components.ComponentMetadataRepo;
import org.infinispan.interceptors.AsyncInterceptor;
import org.infinispan.interceptors.AsyncInterceptorChain;
import org.infinispan.interceptors.InterceptorChainTest;
import org.infinispan.interceptors.impl.AsyncInterceptorChainImpl;
import org.infinispan.test.AbstractInfinispanTest;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;

/**
 * @author Dan Berindei
 * @since 9.0
 */
@Test(groups = "unit", testName = "context.AsyncInvocationContextTest")
public class AsyncInvocationContextTest extends AbstractInfinispanTest {

   private VisitableCommand command = new GetKeyValueCommand("k", 0);
   private VisitableCommand subCommand = new LockControlCommand("k", null, 0, null);

   public void testSingleInterceptor() {
      AsyncInterceptor[] interceptors = {(ctx, command) -> ctx.shortCircuit(null)};
      AsyncInterceptorChain chain = newInterceptorChain(interceptors);
      InvocationContext context = newInvocationContext();

      Object returnValue = chain.invoke(context, command);
      assertEquals(null, returnValue);
   }

   public void testSingleAsyncInterceptor() throws ExecutionException, InterruptedException {
      CompletableFuture<Object> f = new CompletableFuture<>();
      AsyncInterceptor[] interceptors = {(ctx, command) -> f.handle((rv, t) -> null)};
      AsyncInterceptorChain chain = newInterceptorChain(interceptors);
      InvocationContext context = newInvocationContext();

      CompletableFuture<Object> invokeFuture = chain.invokeAsync(context, command);
      assertFalse(invokeFuture.isDone());

      f.complete(null);
      assertEquals(null, invokeFuture.get());
   }

   public void testOnReturnSync() {
      AsyncInterceptor[] interceptors = {(ctx, command) -> ctx
            .onReturn((rCtx, rCommand, rv, t) -> CompletableFuture.completedFuture("v"))};
      AsyncInterceptorChain chain = newInterceptorChain(interceptors);
      InvocationContext context = newInvocationContext();

      Object returnValue = chain.invoke(context, command);
      assertEquals("v", returnValue);
   }

   public void testOnReturnAsync() throws ExecutionException, InterruptedException {
      CompletableFuture<Object> f = new CompletableFuture<>();
      AsyncInterceptor[] interceptors = {(ctx, command) -> ctx.onReturn((rCtx, rCommand, rv, t) -> f)};
      AsyncInterceptorChain chain = newInterceptorChain(interceptors);
      InvocationContext context = newInvocationContext();

      CompletableFuture<Object> invokeFuture = chain.invokeAsync(context, command);
      assertFalse(invokeFuture.isDone());

      f.complete("v");
      assertEquals("v", invokeFuture.get());
   }

   public void testContinueInvocation() throws ExecutionException, InterruptedException {
      CompletableFuture<Object> f = new CompletableFuture<>();
      AsyncInterceptor[] interceptors = {(ctx, command) -> f.thenCompose(v -> ctx.continueInvocation())};
      AsyncInterceptorChain chain = newInterceptorChain(interceptors);
      InvocationContext context = newInvocationContext();

      CompletableFuture<Object> invokeFuture = chain.invokeAsync(context, command);
      assertFalse(invokeFuture.isDone());

      f.complete("v");
      assertEquals(null, invokeFuture.get());
   }

   public void testShortCircuit() {
      AsyncInterceptor[] interceptors =
            {(ctx, command) -> ctx.shortCircuit("v1"), (ctx, command) -> ctx.shortCircuit("v2")};
      AsyncInterceptorChain chain = newInterceptorChain(interceptors);
      InvocationContext context = newInvocationContext();

      Object returnValue = chain.invoke(context, command);
      assertEquals("v1", returnValue);
   }

   public void testShortCircuitAsync() throws ExecutionException, InterruptedException {
      CompletableFuture<Object> f = new CompletableFuture<>();
      AsyncInterceptor[] interceptors = {(ctx, command) -> f.thenCompose(v -> ctx.shortCircuit("v1")),
                                              (ctx, command) -> ctx.shortCircuit("v2")};
      AsyncInterceptorChain chain = newInterceptorChain(interceptors);
      InvocationContext context = newInvocationContext();

      CompletableFuture<Object> invokeFuture = chain.invokeAsync(context, command);
      assertFalse(invokeFuture.isDone());

      f.complete("v");
      assertEquals("v1", invokeFuture.get());
   }

   public void testForkInvocationSync() {
      AsyncInterceptor[] interceptors = {(ctx, command) -> {
         Object subResult = ctx.forkInvocationSync(subCommand);
         return ctx.shortCircuit(subResult);
      }, (ctx, command) -> ctx
            .shortCircuit(command instanceof LockControlCommand ? "subCommand" : "command")};
      AsyncInterceptorChain chain = newInterceptorChain(interceptors);
      InvocationContext context = newInvocationContext();

      Object returnValue = chain.invoke(context, command);
      assertEquals("subCommand", returnValue);
   }

   public void testForkInvocation() {
      AsyncInterceptor[] interceptors = {(ctx, command) -> ctx
            .forkInvocation(subCommand, (rCtx, rCommand, rv, t) -> ctx.shortCircuit(rv)),
                                              (ctx, command) -> ctx
            .shortCircuit(command instanceof LockControlCommand ? "subCommand" : "command")};
      AsyncInterceptorChain chain = newInterceptorChain(interceptors);
      InvocationContext context = newInvocationContext();

      Object returnValue = chain.invoke(context, command);
      assertEquals("subCommand", returnValue);
   }

   protected SingleKeyNonTxInvocationContext newInvocationContext() {
      // Actual implementation doesn't matter, we are only testing the BaseAsyncInvocationContext methods
      return new SingleKeyNonTxInvocationContext(null, AnyEquivalence.getInstance());
   }

   protected AsyncInterceptorChain newInterceptorChain(AsyncInterceptor[] interceptors) {
      ComponentMetadataRepo componentMetadataRepo = new ComponentMetadataRepo();
      componentMetadataRepo.initialize(Collections.emptyList(), InterceptorChainTest.class.getClassLoader());

      AsyncInterceptorChain chain = new AsyncInterceptorChainImpl(componentMetadataRepo);
      for (AsyncInterceptor i : interceptors) {
         chain.appendInterceptor(i, false);
      }
      return chain;
   }
}
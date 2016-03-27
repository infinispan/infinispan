package org.infinispan.context.impl;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.control.LockControlCommand;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commons.equivalence.AnyEquivalence;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.SingleKeyNonTxInvocationContext;
import org.infinispan.factories.components.ComponentMetadataRepo;
import org.infinispan.factories.components.ModuleMetadataFileFinder;
import org.infinispan.interceptors.InterceptorChainTest;
import org.infinispan.interceptors.SequentialInterceptor;
import org.infinispan.interceptors.SequentialInterceptorChain;
import org.infinispan.interceptors.impl.SequentialInterceptorChainImpl;
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
@Test(groups = "unit", testName = "context.SequentialInvocationContextTest")
public class SequentialInvocationContextTest extends AbstractInfinispanTest {

   private VisitableCommand command = new GetKeyValueCommand("k", 0);
   private VisitableCommand subCommand = new LockControlCommand("k", null, 0, null);

   public void testSingleInterceptor() {
      SequentialInterceptor[] interceptors = {(ctx, command) -> ctx.shortCircuit(null)};
      SequentialInterceptorChain chain = newInterceptorChain(interceptors);
      InvocationContext context = newInvocationContext(chain);

      Object returnValue = chain.invoke(context, command);
      assertEquals(null, returnValue);
   }

   public void testSingleAsyncInterceptor() throws ExecutionException, InterruptedException {
      CompletableFuture<Object> f = new CompletableFuture<>();
      SequentialInterceptor[] interceptors = {(ctx, command) -> f.handle((rv, t) -> null)};
      SequentialInterceptorChain chain = newInterceptorChain(interceptors);
      InvocationContext context = newInvocationContext(chain);

      CompletableFuture<Object> invokeFuture = chain.invokeAsync(context, command);
      assertFalse(invokeFuture.isDone());

      f.complete(null);
      assertEquals(null, invokeFuture.get());
   }

   public void testOnReturnSync() {
      SequentialInterceptor[] interceptors = {(ctx, command) -> ctx
            .onReturn((ctx1, command1, rv, t) -> CompletableFuture.completedFuture("v"))};
      SequentialInterceptorChain chain = newInterceptorChain(interceptors);
      InvocationContext context = newInvocationContext(chain);

      Object returnValue = chain.invoke(context, command);
      assertEquals("v", returnValue);
   }

   public void testOnReturnAsync() throws ExecutionException, InterruptedException {
      CompletableFuture<Object> f = new CompletableFuture<>();
      SequentialInterceptor[] interceptors = {(ctx, command) -> ctx.onReturn((ctx1, command1, rv, t) -> f)};
      SequentialInterceptorChain chain = newInterceptorChain(interceptors);
      InvocationContext context = newInvocationContext(chain);

      CompletableFuture<Object> invokeFuture = chain.invokeAsync(context, command);
      assertFalse(invokeFuture.isDone());

      f.complete("v");
      assertEquals("v", invokeFuture.get());
   }

   public void testContinueInvocation() throws ExecutionException, InterruptedException {
      CompletableFuture<Object> f = new CompletableFuture<>();
      SequentialInterceptor[] interceptors = {(ctx, command) -> f.thenCompose(v -> ctx.continueInvocation())};
      SequentialInterceptorChain chain = newInterceptorChain(interceptors);
      InvocationContext context = newInvocationContext(chain);

      CompletableFuture<Object> invokeFuture = chain.invokeAsync(context, command);
      assertFalse(invokeFuture.isDone());

      f.complete("v");
      assertEquals(null, invokeFuture.get());
   }

   public void testShortCircuit() {
      SequentialInterceptor[] interceptors =
            {(ctx, command) -> ctx.shortCircuit("v1"), (ctx, command) -> ctx.shortCircuit("v2")};
      SequentialInterceptorChain chain = newInterceptorChain(interceptors);
      InvocationContext context = newInvocationContext(chain);

      Object returnValue = chain.invoke(context, command);
      assertEquals("v1", returnValue);
   }

   public void testShortCircuitAsync() throws ExecutionException, InterruptedException {
      CompletableFuture<Object> f = new CompletableFuture<>();
      SequentialInterceptor[] interceptors = {(ctx, command) -> f.thenCompose(v -> ctx.shortCircuit("v1")),
                                              (ctx, command) -> ctx.shortCircuit("v2")};
      SequentialInterceptorChain chain = newInterceptorChain(interceptors);
      InvocationContext context = newInvocationContext(chain);

      CompletableFuture<Object> invokeFuture = chain.invokeAsync(context, command);
      assertFalse(invokeFuture.isDone());

      f.complete("v");
      assertEquals("v1", invokeFuture.get());
   }

   public void testForkInvocationSync() {
      SequentialInterceptor[] interceptors = {(ctx, command) -> {
         Object subResult = ctx.forkInvocationSync(subCommand);
         return ctx.shortCircuit(subResult);
      }, (ctx, command) -> ctx
            .shortCircuit(command instanceof LockControlCommand ? "subCommand" : "command")};
      SequentialInterceptorChain chain = newInterceptorChain(interceptors);
      InvocationContext context = newInvocationContext(chain);

      Object returnValue = chain.invoke(context, command);
      assertEquals("subCommand", returnValue);
   }

   public void testForkInvocation() {
      SequentialInterceptor[] interceptors = {(ctx, command) -> ctx
            .forkInvocation(subCommand, (ctx1, command1, rv, t) -> ctx.shortCircuit(rv)),
                                              (ctx, command) -> ctx
            .shortCircuit(command instanceof LockControlCommand ? "subCommand" : "command")};
      SequentialInterceptorChain chain = newInterceptorChain(interceptors);
      InvocationContext context = newInvocationContext(chain);

      Object returnValue = chain.invoke(context, command);
      assertEquals("subCommand", returnValue);
   }

   protected SingleKeyNonTxInvocationContext newInvocationContext(SequentialInterceptorChain chain) {
      // Actual implementation doesn't matter, we are only testing the BaseSequentialInvocationContext methods
      return new SingleKeyNonTxInvocationContext(null, AnyEquivalence.getInstance());
   }

   protected SequentialInterceptorChain newInterceptorChain(SequentialInterceptor[] interceptors) {
      ComponentMetadataRepo componentMetadataRepo = new ComponentMetadataRepo();
      componentMetadataRepo.initialize(Collections.<ModuleMetadataFileFinder>emptyList(),
            InterceptorChainTest.class.getClassLoader());

      SequentialInterceptorChain chain = new SequentialInterceptorChainImpl(componentMetadataRepo);
      for (SequentialInterceptor i : interceptors) {
         chain.appendInterceptor(i, false);
      }
      return chain;
   }
}
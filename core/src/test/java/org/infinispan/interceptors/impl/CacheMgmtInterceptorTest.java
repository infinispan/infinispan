package org.infinispan.interceptors.impl;

import static org.infinispan.interceptors.BaseAsyncInterceptor.makeStage;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNull;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.read.GetAllCommand;
import org.infinispan.commands.read.GetCacheEntryCommand;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.SingleKeyNonTxInvocationContext;
import org.infinispan.interceptors.BaseAsyncInterceptor;
import org.infinispan.interceptors.InvocationStage;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.commons.test.Exceptions;
import org.infinispan.test.TestException;
import org.infinispan.test.TestingUtil;
import org.infinispan.util.ControlledTimeService;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "interceptors.impl.CacheMgmtInterceptorTest")
public class CacheMgmtInterceptorTest extends AbstractInfinispanTest {
   public static final String KEY = "key";
   public static final String VALUE = "value";

   private CacheMgmtInterceptor interceptor;
   private ControlledNextInterceptor nextInterceptor;
   private ControlledTimeService timeService;
   private InvocationContext ctx;

   @BeforeMethod(alwaysRun = true)
   public void setup() {
      nextInterceptor = new ControlledNextInterceptor();
      timeService = new ControlledTimeService();
      ctx = new SingleKeyNonTxInvocationContext(null);

      interceptor = new CacheMgmtInterceptor();
      interceptor.setNextInterceptor(nextInterceptor);
      TestingUtil.inject(interceptor, timeService);
      interceptor.start();
      interceptor.setStatisticsEnabled(true);
   }

   public void testVisitGetKeyValueCommand() throws Throwable {
      GetKeyValueCommand command = new GetKeyValueCommand(KEY, 0, 0);
      InvocationStage stage = makeStage(interceptor.visitGetKeyValueCommand(ctx, command));
      assertFalse(stage.isDone());

      timeService.advance(1);
      nextInterceptor.completeLastInvocation(null);

      assertNull(stage.get());
      assertEquals(1, interceptor.getAverageReadTime());
   }

   public void testVisitGetKeyValueCommandException() throws Throwable {
      GetKeyValueCommand command = new GetKeyValueCommand(KEY, 0, 0);
      InvocationStage stage = makeStage(interceptor.visitGetKeyValueCommand(ctx, command));
      assertFalse(stage.isDone());

      timeService.advance(1);
      nextInterceptor.completeLastInvocationExceptionally(new TestException());

      expectInvocationException(stage);
      assertEquals(1, interceptor.getAverageReadTime());
   }

   public void testVisitGetCacheEntryCommand() throws Throwable {
      GetCacheEntryCommand command = new GetCacheEntryCommand(KEY, 0, 0);
      InvocationStage stage = makeStage(interceptor.visitGetCacheEntryCommand(ctx, command));
      assertFalse(stage.isDone());

      timeService.advance(1);
      nextInterceptor.completeLastInvocation(null);

      assertNull(stage.get());
      assertEquals(1, interceptor.getAverageReadTime());
   }

   public void testVisitGetCacheEntryCommandException() throws Throwable {
      GetCacheEntryCommand command = new GetCacheEntryCommand(KEY, 0, 0);
      InvocationStage stage = makeStage(interceptor.visitGetCacheEntryCommand(ctx, command));
      assertFalse(stage.isDone());

      timeService.advance(1);
      nextInterceptor.completeLastInvocationExceptionally(new TestException());

      expectInvocationException(stage);
      assertEquals(1, interceptor.getAverageReadTime());
   }

   public void testVisitGetAllCommand() throws Throwable {
      GetAllCommand command = new GetAllCommand(Collections.singleton(KEY), 0, false);
      InvocationStage stage = makeStage(interceptor.visitGetAllCommand(ctx, command));
      assertFalse(stage.isDone());

      timeService.advance(1);
      nextInterceptor.completeLastInvocation(Collections.emptyMap());

      assertEquals(Collections.emptyMap(), stage.get());
      assertEquals(1, interceptor.getAverageReadTime());
   }

   public void testVisitGetAllCommandException() throws Throwable {
      GetAllCommand command = new GetAllCommand(Collections.singleton(KEY), 0, false);
      InvocationStage stage = makeStage(interceptor.visitGetAllCommand(ctx, command));
      assertFalse(stage.isDone());

      timeService.advance(1);
      nextInterceptor.completeLastInvocationExceptionally(new TestException());

      expectInvocationException(stage);
      assertEquals(1, interceptor.getAverageReadTime());
   }

   public void testVisitPutMapCommand() throws Throwable {
      PutMapCommand command = new PutMapCommand(Collections.singletonMap(KEY, VALUE), null, 0, null);
      InvocationStage stage = makeStage(interceptor.visitPutMapCommand(ctx, command));
      assertFalse(stage.isDone());

      timeService.advance(1);
      nextInterceptor.completeLastInvocation(null);

      assertNull(stage.get());
      assertEquals(1, interceptor.getAverageWriteTime());
   }

   public void testVisitPutMapCommandException() throws Throwable {
      PutMapCommand command = new PutMapCommand(Collections.singletonMap(KEY, VALUE), null, 0, null);
      InvocationStage stage = makeStage(interceptor.visitPutMapCommand(ctx, command));
      assertFalse(stage.isDone());

      timeService.advance(1);
      nextInterceptor.completeLastInvocationExceptionally(new TestException());

      expectInvocationException(stage);
      assertEquals(1, interceptor.getAverageWriteTime());
   }

   public void testVisitPutKeyValueCommand() throws Throwable {
      PutKeyValueCommand command = new PutKeyValueCommand(KEY, VALUE, false, false, null, 0, 0, null);
      InvocationStage stage = makeStage(interceptor.visitPutKeyValueCommand(ctx, command));
      assertFalse(stage.isDone());

      timeService.advance(1);
      nextInterceptor.completeLastInvocation(null);

      assertNull(stage.get());
      assertEquals(1, interceptor.getAverageWriteTime());
   }

   public void testVisitPutKeyValueCommandException() throws Throwable {
      PutKeyValueCommand command = new PutKeyValueCommand(KEY, VALUE, false, false, null, 0, 0, null);
      InvocationStage stage = makeStage(interceptor.visitPutKeyValueCommand(ctx, command));
      assertFalse(stage.isDone());

      timeService.advance(1);
      nextInterceptor.completeLastInvocationExceptionally(new TestException());

      expectInvocationException(stage);
      assertEquals(1, interceptor.getAverageWriteTime());
   }

   public void testVisitReplaceCommand() throws Throwable {
      ReplaceCommand command = new ReplaceCommand(KEY, VALUE, false, false, null, 0, 0, null);
      InvocationStage stage = makeStage(interceptor.visitReplaceCommand(ctx, command));
      assertFalse(stage.isDone());

      timeService.advance(1);
      nextInterceptor.completeLastInvocation(null);

      assertNull(stage.get());
      assertEquals(1, interceptor.getAverageWriteTime());
   }

   public void testVisitReplaceCommandException() throws Throwable {
      ReplaceCommand command = new ReplaceCommand(KEY, VALUE, false, false, null, 0, 0, null);
      InvocationStage stage = makeStage(interceptor.visitReplaceCommand(ctx, command));
      assertFalse(stage.isDone());

      timeService.advance(1);
      nextInterceptor.completeLastInvocationExceptionally(new TestException());

      expectInvocationException(stage);
      assertEquals(1, interceptor.getAverageWriteTime());
   }

   public void testVisitRemoveCommand() throws Throwable {
      RemoveCommand command = new RemoveCommand(KEY, null, false, 0, 0, null);
      InvocationStage stage = makeStage(interceptor.visitRemoveCommand(ctx, command));
      assertFalse(stage.isDone());

      timeService.advance(1);
      nextInterceptor.completeLastInvocation(VALUE);

      assertEquals(VALUE, stage.get());
      assertEquals(1, interceptor.getAverageRemoveTime());
   }

   public void testVisitRemoveCommandException() throws Throwable {
      RemoveCommand command = new RemoveCommand(KEY, null, false, 0, 0, null);
      InvocationStage stage = makeStage(interceptor.visitRemoveCommand(ctx, command));
      assertFalse(stage.isDone());

      timeService.advance(1);
      nextInterceptor.completeLastInvocationExceptionally(new TestException());

      expectInvocationException(stage);
      assertEquals(0, interceptor.getAverageRemoveTime());
   }

   private void expectInvocationException(InvocationStage stage) {
      Exceptions.expectException(TestException.class, () -> {
         try {
            stage.get();
         } catch (Exception e) {
            throw e;
         } catch (Throwable t) {
            throw new Exception(t);
         }
      });
   }

   class ControlledNextInterceptor extends BaseAsyncInterceptor {

      CompletableFuture<Object> lastReturnValue;

      @Override
      public Object visitCommand(InvocationContext ctx, VisitableCommand command) throws Throwable {
         lastReturnValue = new CompletableFuture<>();
         return asyncValue(lastReturnValue);
      }

      public void completeLastInvocation(Object value) {
         lastReturnValue.complete(value);
      }

      public void completeLastInvocationExceptionally(Throwable t) {
         lastReturnValue.completeExceptionally(t);
      }
   }
}

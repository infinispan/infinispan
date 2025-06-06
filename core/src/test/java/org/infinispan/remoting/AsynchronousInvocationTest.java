package org.infinispan.remoting;

import static org.infinispan.test.TestingUtil.extractGlobalComponent;
import static org.infinispan.test.fwk.TestCacheManagerFactory.createClusteredCacheManager;
import static org.infinispan.test.fwk.TestCacheManagerFactory.getDefaultCacheConfiguration;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.infinispan.commands.GlobalRpcCommand;
import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.module.TestGlobalConfigurationBuilder;
import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.inboundhandler.DeliverOrder;
import org.infinispan.remoting.inboundhandler.InboundInvocationHandler;
import org.infinispan.remoting.inboundhandler.Reply;
import org.infinispan.remoting.responses.ExceptionResponse;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestException;
import org.infinispan.util.ByteString;
import org.infinispan.util.concurrent.BlockingTaskAwareExecutorService;
import org.infinispan.util.concurrent.BlockingTaskAwareExecutorServiceImpl;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Tests the Asynchronous Invocation API and checks if the commands are correctly processed (or JGroups or Infinispan
 * thread pool)
 *
 * @author Pedro Ruivo
 * @since 5.3
 */
@Test(groups = "functional", testName = "remoting.AsynchronousInvocationTest")
public class AsynchronousInvocationTest extends AbstractInfinispanTest {
   public static final String CACHE_NAME = "testCache";
   public static final ByteString CACHE_NAME_BYTES = ByteString.fromString(CACHE_NAME);

   private EmbeddedCacheManager cacheManager;
   private DummyTaskCountExecutorService nonBlockingExecutorService;
   private DummyTaskCountExecutorService blockingExecutorService;
   private InboundInvocationHandler invocationHandler;
   private Address address;

   private static CacheRpcCommand mockCacheRpcCommand() throws Throwable {
      CacheRpcCommand mock = mock(CacheRpcCommand.class);
      when(mock.getCacheName()).thenReturn(CACHE_NAME_BYTES);
      when(mock.invokeAsync(any())).thenReturn(CompletableFutures.completedNull());
      return mock;
   }

   private static GlobalRpcCommand mockGlobalRpcCommand() throws Throwable {
      GlobalRpcCommand mock = mock(GlobalRpcCommand.class);
      when(mock.invokeAsync(any())).thenReturn(CompletableFutures.completedNull());
      return mock;
   }

   @BeforeClass
   public void setUp() throws Throwable {
      // We need to use an actual thread pool - due to a circular dependency in ClusterTopologyManagerImpl invoking
      // a command via the non blocking executor that loads up the LocalTopologyManagerImpl that Injects the ClusterTopologyManagerImpl
      ExecutorService realExecutor = Executors.newSingleThreadExecutor();
      nonBlockingExecutorService = new DummyTaskCountExecutorService(realExecutor);
      blockingExecutorService = new DummyTaskCountExecutorService(realExecutor);
      BlockingTaskAwareExecutorService nonBlockingExecutor =
         new BlockingTaskAwareExecutorServiceImpl(nonBlockingExecutorService,
                                                  TIME_SERVICE);
      BlockingTaskAwareExecutorService blockingExecutor =
            new BlockingTaskAwareExecutorServiceImpl(blockingExecutorService,
                  TIME_SERVICE);
      GlobalConfigurationBuilder globalBuilder = GlobalConfigurationBuilder.defaultClusteredBuilder();
      globalBuilder.defaultCacheName(CACHE_NAME);
      globalBuilder.addModule(TestGlobalConfigurationBuilder.class)
                   .testGlobalComponent(KnownComponentNames.NON_BLOCKING_EXECUTOR, nonBlockingExecutor)
                   .testGlobalComponent(KnownComponentNames.BLOCKING_EXECUTOR, blockingExecutor);
      ConfigurationBuilder builder = getDefaultCacheConfiguration(false);
      builder.clustering().cacheMode(CacheMode.DIST_SYNC);
      cacheManager = createClusteredCacheManager(globalBuilder, builder);
      Transport transport = extractGlobalComponent(cacheManager, Transport.class);
      address = transport.getAddress();
      invocationHandler = extractGlobalComponent(cacheManager, InboundInvocationHandler.class);

      // Start the cache
      cacheManager.getCache();
   }

   @AfterClass
   public void tearDown() {
      if (cacheManager != null) {
         // BlockingTaskAwareExecutorServiceImpl doesn't have a @Stop annotation so we need to stop it manually
         extractGlobalComponent(cacheManager, ExecutorService.class, KnownComponentNames.NON_BLOCKING_EXECUTOR).shutdownNow();
         extractGlobalComponent(cacheManager, ExecutorService.class, KnownComponentNames.BLOCKING_EXECUTOR).shutdownNow();
         cacheManager.stop();
      }
   }

   public void testCacheRpcCommands() throws Throwable {
      CacheRpcCommand nonBlockingCacheRpcCommand = mockCacheRpcCommand();
      assertDispatchForCommand(nonBlockingCacheRpcCommand);
   }

   public void testGlobalRpcCommands() throws Throwable {
      GlobalRpcCommand nonBlockingGlobalRpcCommand = mockGlobalRpcCommand();
      assertDispatchForCommand(nonBlockingGlobalRpcCommand);
   }

   private void assertDispatchForCommand(ReplicableCommand command) throws Exception {
      log.debugf("Testing " + command.getClass().getCanonicalName());
      DummyTaskCountExecutorService executorToUse = nonBlockingExecutorService;
      executorToUse.reset();
      CompletableFutureResponse response = new CompletableFutureResponse();
      invocationHandler.handleFromCluster(address, command, response, DeliverOrder.NONE);
      response.await(30, TimeUnit.SECONDS);
      assertFalse(executorToUse.hasExecutedCommand, "Command " + command.getClass() + " dispatched wrongly.");

      executorToUse.reset();
      response = new CompletableFutureResponse();
      invocationHandler.handleFromCluster(address, command, response, DeliverOrder.PER_SENDER);
      response.await(30, TimeUnit.SECONDS);
      assertFalse(executorToUse.hasExecutedCommand, "Command " + command.getClass() + " dispatched wrongly.");
   }

   private static class DummyTaskCountExecutorService extends AbstractExecutorService {

      private final ExecutorService realExecutor;
      private volatile boolean hasExecutedCommand;

      private DummyTaskCountExecutorService(ExecutorService realExecutor) {
         this.realExecutor = realExecutor;
      }

      @Override
      public void execute(Runnable command) {
         hasExecutedCommand = true;
         realExecutor.execute(command);
      }

      public void reset() {
         hasExecutedCommand = false;
      }

      @Override
      public void shutdown() {
         realExecutor.shutdown();
      }

      @Override
      public List<Runnable> shutdownNow() {
         return realExecutor.shutdownNow();
      }

      @Override
      public boolean isShutdown() {
         return realExecutor.isShutdown();
      }

      @Override
      public boolean isTerminated() {
         return realExecutor.isTerminated();
      }

      @Override
      public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
         return realExecutor.awaitTermination(timeout, unit);
      }
   }

   private static class CompletableFutureResponse implements Reply {

      private final CompletableFuture<Response> responseFuture = new CompletableFuture<>();

      public void await(long time, TimeUnit unit) throws Exception {
         Response response = responseFuture.get(time, unit);
         if (response instanceof ExceptionResponse) {
            throw new TestException(((ExceptionResponse) response).getException());
         }
      }

      @Override
      public void reply(Response response) {
         responseFuture.complete(response);
      }
   }
}

package org.infinispan.util;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;

import org.infinispan.commons.IllegalLifecycleStateException;
import org.infinispan.commons.internal.CommonsBlockHoundIntegration;
import org.infinispan.commons.test.PolarionJUnitXMLWriter;
import org.infinispan.commons.test.TestResourceTracker;
import org.infinispan.commons.test.TestSuiteProgress;
import org.infinispan.conflict.impl.ConflictManagerTest;
import org.infinispan.distribution.BlockingInterceptor;
import org.infinispan.eviction.impl.EvictionWithConcurrentOperationsTest;
import org.infinispan.functional.FunctionalTestUtils;
import org.infinispan.manager.DefaultCacheManagerHelper;
import org.infinispan.notifications.cachelistener.CacheListenerVisibilityTest;
import org.infinispan.persistence.support.WaitNonBlockingStore;
import org.infinispan.test.ReplListener;
import org.infinispan.test.TestBlocking;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.concurrent.InboundRpcSequencerAction;
import org.infinispan.test.concurrent.StateSequencer;
import org.infinispan.test.fwk.CheckPoint;
import org.infinispan.util.concurrent.ReclosableLatch;
import org.infinispan.util.logging.LogFactory;
import org.kohsuke.MetaInfServices;

import io.reactivex.rxjava3.exceptions.UndeliverableException;
import io.reactivex.rxjava3.plugins.RxJavaPlugins;
import reactor.blockhound.BlockHound;
import reactor.blockhound.integration.BlockHoundIntegration;

@SuppressWarnings("unused")
@MetaInfServices
public class CoreTestBlockHoundIntegration implements BlockHoundIntegration {
   @Override
   public void applyTo(BlockHound.Builder builder) {
      try {
         allowTestsToBlock(builder);
      } catch (ClassNotFoundException e) {
         throw new AssertionError(e);
      }

      DefaultCacheManagerHelper.enableManagerGetCacheBlockingCheck();

      builder.allowBlockingCallsInside(CoreTestBlockHoundIntegration.class.getName(), "writeJUnitReport");

      builder.blockingMethodCallback(bm -> {
         String testName = TestResourceTracker.getCurrentTestName();
         AssertionError assertionError = new AssertionError(String.format("Blocking call! %s on thread %s", bm, Thread.currentThread()));
         TestSuiteProgress.fakeTestFailure(testName + ".BlockingChecker", assertionError);
         writeJUnitReport(testName, assertionError, "Blocking");
         throw assertionError;
      });

      Thread.setDefaultUncaughtExceptionHandler((thread, t) -> {
         LogFactory.getLogger("Infinispan-TEST").fatal("Throwable was not caught in thread " + thread +
               " - exception is: " + t);
         // RxJava propagates via this and we don't want to worry about it
         if (!(t instanceof UndeliverableException)) {
            writeJUnitReport(TestResourceTracker.getCurrentTestName(), t, "Uncaught");
         }
      });

      RxJavaPlugins.setErrorHandler(t -> {
         // RxJavaPlugins wraps some but not all exceptions in an UndeliverableException
         Throwable throwable = t instanceof UndeliverableException ? t.getCause() : t;

         // Ignore lifecycle exceptions as this can happen when shutting down executors etc.
         if (throwable instanceof IllegalLifecycleStateException) {
            return;
         }

         writeJUnitReport(TestResourceTracker.getCurrentTestName(), throwable, "Undelivered");
      });
   }

   private static void allowTestsToBlock(BlockHound.Builder builder) throws ClassNotFoundException {
      builder.allowBlockingCallsInside(EvictionWithConcurrentOperationsTest.class.getName() + "$Latch", "blockIfNeeded");
      CommonsBlockHoundIntegration.allowPublicMethodsToBlock(builder, CheckPoint.class);
      builder.allowBlockingCallsInside(BlockingInterceptor.class.getName(), "blockIfNeeded");
      builder.allowBlockingCallsInside(TestingUtil.class.getName(), "sleepRandom");
      builder.allowBlockingCallsInside(TestingUtil.class.getName(), "sleepThread");
      CommonsBlockHoundIntegration.allowPublicMethodsToBlock(builder, ReclosableLatch.class);
      builder.allowBlockingCallsInside(BlockingLocalTopologyManager.class.getName() + "$Event", "awaitUnblock");
      builder.allowBlockingCallsInside(BlockingLocalTopologyManager.class.getName() + "$Event", "unblock");

      builder.allowBlockingCallsInside(ControlledRpcManager.class.getName(), "performRequest");
      builder.allowBlockingCallsInside(ControlledRpcManager.class.getName(), "expectCommandAsync");

      builder.allowBlockingCallsInside(ControlledTransport.class.getName(), "performRequest");
      builder.allowBlockingCallsInside(ControlledTransport.class.getName(), "expectCommandAsync");

      CommonsBlockHoundIntegration.allowPublicMethodsToBlock(builder, StateSequencer.class);
      CommonsBlockHoundIntegration.allowPublicMethodsToBlock(builder, NotifierLatch.class);

      CommonsBlockHoundIntegration.allowPublicMethodsToBlock(builder, TestBlocking.class);

      builder.allowBlockingCallsInside(FunctionalTestUtils.class.getName(), "await");

      builder.allowBlockingCallsInside(TestingUtil.class.getName(), "sleepThread");

      CommonsBlockHoundIntegration.allowMethodsToBlock(builder, Class.forName(ReplListener.class.getName() + "$ReplListenerInterceptor"), false);
      // This uses a lambda callback to invoke some methods - which aren't public
      CommonsBlockHoundIntegration.allowMethodsToBlock(builder, Class.forName(InboundRpcSequencerAction.class.getName() + "$SequencerPerCacheInboundInvocationHandler"), false);

      builder.allowBlockingCallsInside(CacheListenerVisibilityTest.EntryModifiedWithAssertListener.class.getName(), "entryCreated");
      builder.allowBlockingCallsInside(CacheListenerVisibilityTest.EntryCreatedWithAssertListener.class.getName(), "entryCreated");

      CommonsBlockHoundIntegration.allowPublicMethodsToBlock(builder, BlockingLocalTopologyManager.class);
      CommonsBlockHoundIntegration.allowPublicMethodsToBlock(builder, AbstractControlledLocalTopologyManager.class);

      CommonsBlockHoundIntegration.allowPublicMethodsToBlock(builder, ConflictManagerTest.DelayStateResponseCommandHandler.class);

      // The join is used to allow for a sync API for test simplicity - where as the actual store invocation
      // must be non blocking
      builder.allowBlockingCallsInside(WaitNonBlockingStore.class.getName(), "join");
   }

   private static void writeJUnitReport(String testName, Throwable throwable, String type) {
      try {
         File reportsDir = new File("target/surefire-reports");
         if (!reportsDir.exists() && !reportsDir.mkdirs()) {
            throw new IOException("Cannot create report directory " + reportsDir.getAbsolutePath());
         }
         PolarionJUnitXMLWriter writer = new PolarionJUnitXMLWriter(
               new File(reportsDir, "TEST-" + testName + "-" + type + ".xml"));
         String property = System.getProperty("infinispan.modulesuffix");
         String moduleName = property != null ? property.substring(1) : "";
         writer.start(moduleName, 1, 0, 1, 0, false);

         StringWriter exceptionWriter = new StringWriter();
         throwable.printStackTrace(new PrintWriter(exceptionWriter));
         writer.writeTestCase(type, testName, 0, PolarionJUnitXMLWriter.Status.FAILURE,
               exceptionWriter.toString(), throwable.getClass().getName(), throwable.getMessage());

         writer.close();
      } catch (Exception e) {
         throw new RuntimeException("Error reporting " + type, e);
      }
   }
}

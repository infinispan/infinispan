package org.infinispan.client.hotrod.test;

import java.util.Properties;
import java.util.concurrent.ThreadPoolExecutor;

import org.infinispan.client.hotrod.impl.async.DefaultAsyncExecutorFactory;
import org.infinispan.test.fwk.TestResourceTracker;

public class TestAsyncExecutorFactory extends DefaultAsyncExecutorFactory {
   @Override
   public ThreadPoolExecutor getExecutor(Properties p) {
      ThreadPoolExecutor executor = super.getExecutor(p);
      String testName = TestResourceTracker.getCurrentTestName();
      int lastDot = testName.lastIndexOf('.');
      if (lastDot >= 0) {
         testName = testName.substring(lastDot + 1);
      }
      String prefix = testName + "-HR-async-";
      executor.setThreadFactory(r -> {
         Thread th = new Thread(r, prefix + counter.getAndIncrement());
         th.setDaemon(true);
         return th;
      });
      return executor;
   }
}

package org.infinispan.server.task;

import static org.infinispan.test.TestingUtil.withCacheManager;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.commons.executors.BlockingResource;
import org.infinispan.commons.test.junit.JUnitThreadTrackerRule;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.server.tasks.ServerTaskEngine;
import org.infinispan.server.tasks.ServerTaskWrapper;
import org.infinispan.tasks.ServerTask;
import org.infinispan.tasks.TaskContext;
import org.infinispan.tasks.TaskManager;
import org.infinispan.test.CacheManagerCallable;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.junit.ClassRule;
import org.junit.Test;

public class ServerTaskEngineBlockingTest {

   @ClassRule
   public static final JUnitThreadTrackerRule tracker = new JUnitThreadTrackerRule();

   @Test
   public void testBlockingTaskOnBlockingManager() {
      withCacheManager(new CacheManagerCallable(TestCacheManagerFactory.createCacheManager(new GlobalConfigurationBuilder(), new ConfigurationBuilder())) {
         @Override
         public void call() throws Exception {
            AtomicInteger invocations = new AtomicInteger(0);
            String taskName = "blocking-task";
            ServerTask<Void> task = new ServerTask<>() {

               @Override
               public Void call() {
                  assertTrue(Thread.currentThread().getThreadGroup() instanceof BlockingResource);
                  invocations.incrementAndGet();
                  return null;
               }

               @Override
               public String getName() {
                  return taskName;
               }

               @Override
               public void setTaskContext(TaskContext taskContext) { }

               @Override
               public String getType() {
                  return "blocking";
               }
            };
            ServerTaskEngine ste = new ServerTaskEngine(cm, Map.of(taskName, new ServerTaskWrapper<>(task)));
            TaskManager tm = TestingUtil.extractGlobalComponent(cm, TaskManager.class);
            tm.registerTaskEngine(ste);

            assertEquals(0, invocations.get());
            CompletionStage<Void> cs = tm.runTask(taskName, new TaskContext());

            cs.toCompletableFuture().get(10, TimeUnit.SECONDS);
            assertTrue(invocations.get() > 0);
         }
      });
   }
}

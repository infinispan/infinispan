package org.infinispan.server.core.transport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import io.netty.channel.Channel;
import io.netty.channel.DefaultEventLoopGroup;
import io.netty.channel.EventLoop;
import io.netty.channel.local.LocalChannel;
import io.netty.util.concurrent.EventExecutor;

@Test(groups = "unit", testName = "server.core.transport.NonRecursiveEventLoopGroupTest")
public class NonRecursiveEventLoopGroupTest {
   private static final int EVENT_LOOPS = 4;

   private DefaultEventLoopGroup eventLoopGroup;
   private NonRecursiveEventLoopGroup group;

   @BeforeMethod
   public void setUp() {
      eventLoopGroup = new DefaultEventLoopGroup(EVENT_LOOPS);
      group = new NonRecursiveEventLoopGroup(eventLoopGroup);
   }

   @AfterMethod(alwaysRun = true)
   public void tearDown() throws InterruptedException {
      eventLoopGroup.shutdownGracefully(0, 1, TimeUnit.SECONDS).await(5, TimeUnit.SECONDS);
   }

   public void testTasksDoNotSkewChannelRegistration() throws Exception {
      Set<EventLoop> registeredOn = new HashSet<>();
      for (int i = 0; i < EVENT_LOOPS; i++) {
         // The server bootstrap registers every accepted channel through the group
         Channel channel = new LocalChannel();
         group.register(channel).sync();
         registeredOn.add(channel.eventLoop());
         group.submit(() -> { }).sync();
         channel.close().sync();
      }
      assertEquals(EVENT_LOOPS, registeredOn.size(), "Channels were registered on " + registeredOn);
   }

   public void testTaskNotExecutedOnSubmittingEventLoop() throws Exception {
      for (EventExecutor submitter : eventLoopGroup) {
         Thread submitterThread = submitter.submit(Thread::currentThread).get(10, TimeUnit.SECONDS);
         for (int i = 0; i < EVENT_LOOPS; i++) {
            CompletableFuture<Thread> taskThread = new CompletableFuture<>();
            submitter.execute(() -> group.execute(() -> taskThread.complete(Thread.currentThread())));
            assertNotSame(submitterThread, taskThread.get(10, TimeUnit.SECONDS));
         }
      }
   }
}

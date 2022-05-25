package org.infinispan.remoting;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.infinispan.commons.CacheException;
import org.infinispan.commons.io.ByteBuffer;
import org.infinispan.commons.io.ByteBufferImpl;
import org.infinispan.commons.test.Exceptions;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.manager.EmbeddedCacheManagerStartupException;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.remoting.transport.raft.RaftChannel;
import org.infinispan.remoting.transport.raft.RaftChannelConfiguration;
import org.infinispan.remoting.transport.raft.RaftManager;
import org.infinispan.remoting.transport.raft.RaftStateMachine;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

/**
 * Basic test for RAFT protocol.
 *
 * @since 14.0
 */
@Test(groups = "functional", testName = "remoting.RaftTest")
public class RaftTest extends MultipleCacheManagersTest {

   private static final RaftChannelConfiguration DEFAULT_CONFIGURATION = new RaftChannelConfiguration.Builder()
         .logMode(RaftChannelConfiguration.RaftLogMode.VOLATILE)
         .build();
   private static final int CONCURRENT_THREADS = 16;
   private static final int CLUSTER_SIZE = 3;
   // note, node name must contain the test name!
   private static final String[] RAFT_MEMBERS = new String[]{"RaftTest-A", "RaftTest-B", "RaftTest-C", "RaftTest-D"};

   @Override
   protected void createCacheManagers() throws Throwable {
      for (int i = 0; i < CLUSTER_SIZE; ++i) {
         GlobalConfigurationBuilder builder = defaultGlobalConfigurationBuilder();
         builder.transport().raftMembers(RAFT_MEMBERS);
         builder.transport().nodeName(RAFT_MEMBERS[i]);
         addClusterEnabledCacheManager(builder, null);
      }
   }

   public void testRaft(Method method) throws ExecutionException, InterruptedException, TimeoutException {
      List<RaftManager> raftManagerList = raftManagers();
      for (RaftManager m : raftManagerList) {
         AssertJUnit.assertTrue(m.isRaftAvailable());
      }

      List<RaftQueueStateMachine> stateMachines = registerStateMachine(raftManagerList, RaftQueueStateMachine::new, method.getName());
      awaitForLeader(raftManagerList, method.getName());

      List<Future<CompletionStage<ByteBuffer>>> futures = new ArrayList<>(CONCURRENT_THREADS);
      CyclicBarrier barrier = new CyclicBarrier(CONCURRENT_THREADS);

      for (int i = 0; i < CONCURRENT_THREADS; ++i) {
         int idx = i % stateMachines.size();
         byte b = (byte) i;
         futures.add(fork(() -> {
            barrier.await(10, TimeUnit.SECONDS);
            return stateMachines.get(idx).raftChannel.send(ByteBufferImpl.create(b));
         }));
      }

      for (Future<CompletionStage<ByteBuffer>> f : futures) {
         CompletionStage<ByteBuffer> cf = f.get(10, TimeUnit.SECONDS);
         ByteBuffer buffer = cf.toCompletableFuture().get(10, TimeUnit.SECONDS);
         AssertJUnit.assertEquals(1, buffer.getLength());
         AssertJUnit.assertEquals(0, buffer.getBuf()[0]);
      }

      List<Byte> expectedState = null;
      // wait until all bytes are applied
      for (int i = 0; i < stateMachines.size(); ++i) {
         RaftQueueStateMachine m = stateMachines.get(i);
         eventually(() -> m.state.size() == CONCURRENT_THREADS);
         if (expectedState == null) {
            expectedState = new ArrayList<>(m.state);
         } else {
            AssertJUnit.assertEquals("State is different for node " + i, expectedState, m.state);
         }
      }
   }

   public void testRaftStateTransfer(Method method) throws ExecutionException, InterruptedException, TimeoutException {
      List<RaftManager> raftManagerList = raftManagers();
      for (RaftManager m : raftManagerList) {
         AssertJUnit.assertTrue(m.isRaftAvailable());
      }

      List<RaftQueueStateMachine> stateMachines = registerStateMachine(raftManagerList, RaftQueueStateMachine::new, method.getName());
      awaitForLeader(raftManagerList, method.getName());

      List<Future<CompletionStage<ByteBuffer>>> futures = new ArrayList<>(CONCURRENT_THREADS);
      CyclicBarrier barrier = new CyclicBarrier(CONCURRENT_THREADS);

      for (int i = 0; i < CONCURRENT_THREADS; ++i) {
         int idx = i % stateMachines.size();
         byte b = (byte) i;
         futures.add(fork(() -> {
            barrier.await(10, TimeUnit.SECONDS);
            return stateMachines.get(idx).raftChannel.send(ByteBufferImpl.create(b));
         }));
      }

      for (Future<CompletionStage<ByteBuffer>> f : futures) {
         CompletionStage<ByteBuffer> cf = f.get(10, TimeUnit.SECONDS);
         ByteBuffer buffer = cf.toCompletableFuture().get(10, TimeUnit.SECONDS);
         AssertJUnit.assertEquals(1, buffer.getLength());
         AssertJUnit.assertEquals(0, buffer.getBuf()[0]);
      }

      List<Byte> expectedState = null;
      // wait until all bytes are applied
      for (int i = 0; i < stateMachines.size(); ++i) {
         RaftQueueStateMachine m = stateMachines.get(i);
         eventually(() -> m.state.size() == CONCURRENT_THREADS);
         if (expectedState == null) {
            expectedState = new ArrayList<>(m.state);
         } else {
            AssertJUnit.assertEquals("State is different for node " + i, expectedState, m.state);
         }
      }

      try {
         GlobalConfigurationBuilder builder = defaultGlobalConfigurationBuilder();
         builder.transport().raftMembers(RAFT_MEMBERS);
         builder.transport().nodeName(RAFT_MEMBERS[3]);
         EmbeddedCacheManager cm = addClusterEnabledCacheManager(builder, null);

         RaftManager raftManager = raftManager(cm);
         RaftQueueStateMachine sm = registerStateMachine(raftManager, RaftQueueStateMachine::new, method.getName());
         awaitForLeader(raftManager, method.getName());

         // eventually, receives all entries!
         eventuallyEquals(CONCURRENT_THREADS, sm.state::size);
         AssertJUnit.assertEquals("State is different for node 3", expectedState, sm.state);
      } finally {
         // kill the new member
         if (cacheManagers.size() == 4) {
            TestingUtil.killCacheManagers(cacheManagers.remove(3));
         }
      }
   }

   public void testNoDupes(Method method) throws ExecutionException, InterruptedException, TimeoutException {
      List<RaftManager> raftManagerList = raftManagers();
      for (RaftManager m : raftManagerList) {
         AssertJUnit.assertTrue(m.isRaftAvailable());
      }

      List<RaftQueueStateMachine> stateMachines = registerStateMachine(raftManagerList, RaftQueueStateMachine::new, method.getName());
      awaitForLeader(raftManagerList, method.getName());

      List<Future<CompletionStage<ByteBuffer>>> futures = new ArrayList<>(CONCURRENT_THREADS);
      CyclicBarrier barrier = new CyclicBarrier(CONCURRENT_THREADS);

      for (int i = 0; i < CONCURRENT_THREADS; ++i) {
         int idx = i % stateMachines.size();
         byte b = (byte) i;
         futures.add(fork(() -> {
            barrier.await(10, TimeUnit.SECONDS);
            return stateMachines.get(idx).raftChannel.send(ByteBufferImpl.create(b));
         }));
      }

      for (Future<CompletionStage<ByteBuffer>> f : futures) {
         CompletionStage<ByteBuffer> cf = f.get(10, TimeUnit.SECONDS);
         ByteBuffer buffer = cf.toCompletableFuture().get(10, TimeUnit.SECONDS);
         AssertJUnit.assertEquals(1, buffer.getLength());
         AssertJUnit.assertEquals(0, buffer.getBuf()[0]);
      }

      List<Byte> expectedState = null;
      // wait until all bytes are applied
      for (int i = 0; i < stateMachines.size(); ++i) {
         RaftQueueStateMachine m = stateMachines.get(i);
         eventually(() -> m.state.size() == CONCURRENT_THREADS);
         if (expectedState == null) {
            expectedState = new ArrayList<>(m.state);
         } else {
            AssertJUnit.assertEquals("State is different for node " + i, expectedState, m.state);
         }
      }

      try {
         GlobalConfigurationBuilder builder = defaultGlobalConfigurationBuilder();
         builder.transport().raftMembers(RAFT_MEMBERS);
         // duplicated node name! the start should fail
         builder.transport().nodeName(RAFT_MEMBERS[2]);
         Exceptions.expectException(EmbeddedCacheManagerStartupException.class, CacheException.class, SecurityException.class, () -> addClusterEnabledCacheManager(builder, null));
      } finally {
         // kill the new member
         if (cacheManagers.size() == 4) {
            TestingUtil.killCacheManagers(cacheManagers.remove(3));
         }
      }
   }

   private List<RaftManager> raftManagers() {
      return cacheManagers.stream()
            .map(RaftTest::raftManager)
            .collect(Collectors.toList());
   }

   private static RaftManager raftManager(EmbeddedCacheManager cacheManager) {
      return TestingUtil.extractGlobalComponent(cacheManager, Transport.class).raftManager();
   }

   private static <T extends RaftStateMachine> List<T> registerStateMachine(List<? extends RaftManager> raftManagers, Supplier<? extends T> supplier, String name) {
      return raftManagers.stream()
            .map(m -> registerStateMachine(m, supplier, name))
            .collect(Collectors.toList());
   }

   private static <T extends RaftStateMachine> T registerStateMachine(RaftManager manager, Supplier<T> supplier, String name) {
      return manager.getOrRegisterStateMachine(name, supplier, DEFAULT_CONFIGURATION);
   }

   private static void awaitForLeader(List<? extends RaftManager> raftManagers, String name) {
      for (RaftManager manager : raftManagers) {
         awaitForLeader(manager, name);
      }
   }

   private static void awaitForLeader(RaftManager manager, String name) {
      eventually(() -> manager.hasLeader(name));
   }

   private static class RaftQueueStateMachine implements RaftStateMachine {

      private volatile RaftChannel raftChannel;
      final List<Byte> state = Collections.synchronizedList(new LinkedList<>());

      @Override
      public void init(RaftChannel raftChannel) {
         this.raftChannel = raftChannel;
      }

      @Override
      public ByteBuffer apply(ByteBuffer buffer) throws Exception {
         AssertJUnit.assertEquals(1, buffer.getLength());
         state.add(buffer.getBuf()[0]);
         log.debugf("[%s | %s] apply: %d", raftChannel.channelName(), raftChannel.raftId(), state.size());
         return ByteBufferImpl.create((byte) 0);
      }

      @Override
      public void readStateFrom(DataInput dataInput) throws IOException {
         int size = dataInput.readInt();
         state.clear();
         for (int i = 0; i < size; ++i) {
            state.add(dataInput.readByte());
         }
         log.debugf("[%s | %s] received state: %d", raftChannel.channelName(), raftChannel.raftId(), state.size());
      }

      @Override
      public void writeStateTo(DataOutput dataOutput) throws IOException {
         List<Byte> copy = new ArrayList<>(state);
         dataOutput.writeInt(copy.size());
         for (byte b : copy) {
            dataOutput.writeByte(b);
         }
         log.debugf("[%s | %s] sent state: %d", raftChannel.channelName(), raftChannel.raftId(), copy.size());
      }
   }
}

package org.infinispan.commands.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.infinispan.commands.CommandInvocationId;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.ForgetInvocationsCommand;
import org.infinispan.commands.InvocationManager;
import org.infinispan.configuration.cache.ClusteringConfiguration;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.container.DataContainer;
import org.infinispan.distribution.DistributionInfo;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.persistence.manager.PersistenceManager;
import org.infinispan.remoting.inboundhandler.DeliverOrder;
import org.infinispan.remoting.rpc.ResponseMode;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.rpc.RpcOptions;
import org.infinispan.util.TimeService;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

public class InvocationManagerImpl implements InvocationManager {
   private static final int MAX = Integer.getInteger("infinispan.invocations", 100);

   private static final Log log = LogFactory.getLog(InvocationManagerImpl.class);
   private static final boolean trace = log.isTraceEnabled();

   private Configuration configuration;
   private TimeService timeService;
   private RpcManager rpcManager;
   private CommandsFactory cf;
   private DistributionManager dm;
   private DataContainer<?, ?> dataContainer;
   private PersistenceManager persistenceManager;

   private long invocationTimeout;
   private RpcOptions forgetRpcOptions;

   private Object[][] keys;
   private long[][] ids;
   private int[] lengths;
   private Object[] locks;

   @Inject
   public void inject(Configuration configuration, TimeService timeService, RpcManager rpcManager, CommandsFactory cf,
                      DistributionManager dm, DataContainer dataContainer, PersistenceManager persistenceManager) {
      this.configuration = configuration;
      this.timeService = timeService;
      this.rpcManager = rpcManager;
      this.cf = cf;
      this.dm = dm;
      this.dataContainer = dataContainer;
      this.persistenceManager = persistenceManager;
   }

   @Start
   public void start() {
      long value = configuration.clustering().remoteTimeout() * 4;
      configuration.clustering().attributes().attribute(ClusteringConfiguration.REMOTE_TIMEOUT)
            .addListener((attribute, oldValue) -> invocationTimeout = configuration.clustering().remoteTimeout() * 4);
      invocationTimeout = value;

      int numSegments = configuration.clustering().hash().numSegments();
      keys = new Object[numSegments][];
      ids = new long[numSegments][];
      lengths = new int[numSegments];
      locks = new Object[numSegments];

      for (int i = 0; i < numSegments; ++i) {
         keys[i] = new Object[MAX];
         ids[i] = new long[MAX];
         locks[i] = new Object();
      }

      forgetRpcOptions = rpcManager.getRpcOptionsBuilder(ResponseMode.ASYNCHRONOUS, DeliverOrder.NONE).build();
   }

   @Override
   public long invocationTimeout() {
      return invocationTimeout;
   }

   @Override
   public long wallClockTime() {
      return timeService.wallClockTime();
   }

   @Override
   public boolean isSynchronous() {
      return configuration.clustering().cacheMode().isSynchronous();
   }

   @Override
   public void notifyCompleted(CommandInvocationId commandInvocationId, Object key, int segment) {
      assert rpcManager.getAddress().equals(commandInvocationId.getAddress());
      ForgetInvocationsCommand fic = null;
      synchronized (locks[segment]) {
         int pos = lengths[segment];
         if (trace) {
            log.tracef("Segment %d has currently %d completed commands, another one being completed", segment, pos);
         }
         if (pos == MAX) {
            Object[] keys = Arrays.copyOf(this.keys[segment], MAX + 1);
            long[] ids = Arrays.copyOf(this.ids[segment], MAX + 1);
            keys[MAX] = key;
            ids[MAX] = commandInvocationId.getId();
            fic = cf.buildForgetInvocationsCommand(keys, ids);
            // we fill the array to release keys for GC, but don't have to zero ids
            Arrays.fill(this.keys[segment], null);
            lengths[segment] = 0;
         } else {
            keys[segment][pos] = key;
            ids[segment][pos] = commandInvocationId.getId();
            lengths[segment] = pos + 1;
         }
      }
      if (fic != null) {
         forgetInvocations(segment, fic, forgetRpcOptions);
      }
   }

   @Override
   public void notifyCompleted(CommandInvocationId commandInvocationId, Collection<?>[] keysBySegment) {
      assert rpcManager.getAddress().equals(commandInvocationId.getAddress());
      for (int segment = 0; segment < keysBySegment.length; ++segment) {
         Collection<?> commandKeys = keysBySegment[segment];
         if (commandKeys == null) {
            continue;
         }
         ForgetInvocationsCommand fic = null;
         synchronized (locks[segment]) {
            int pos = lengths[segment];
            if (trace) {
               log.tracef("Segment %d has currently %d completed commands, another %d are being completed", segment, pos, commandKeys.size());
            }
            int segmentKeys = pos + commandKeys.size();
            if (segmentKeys > MAX) {
               Object[] keys = Arrays.copyOf(this.keys[segment], segmentKeys);
               long[] ids = Arrays.copyOf(this.ids[segment], MAX + 1);
               for (Object key : commandKeys) {
                  keys[pos] = key;
                  ids[pos] = commandInvocationId.getId();
                  ++pos;
               }
               fic = cf.buildForgetInvocationsCommand(keys, ids);
               // we fill the array to release keys for GC, but don't have to zero ids
               Arrays.fill(this.keys[segment], 0, lengths[segment], null);
               lengths[segment] = 0;
            } else {
               Object[] keys = this.keys[segment];
               long[] ids = this.ids[segment];
               for (Object key : commandKeys) {
                  keys[pos] = key;
                  ids[pos] = commandInvocationId.getId();
                  ++pos;
               }
               lengths[segment] = segmentKeys;
            }
         }
         if (fic != null) {
            forgetInvocations(segment, fic, forgetRpcOptions);
         }
      }
   }

   @Override
   public void flush() {
      ArrayList<CompletableFuture<?>> futures = new ArrayList<>(this.keys.length);
      for (int segment = 0; segment < this.keys.length; ++segment) {
         ForgetInvocationsCommand fic;
         synchronized (locks[segment]) {
            int pos = lengths[segment];
            Object[] keys = Arrays.copyOf(this.keys[segment], pos);
            long[] ids = Arrays.copyOf(this.ids[segment], pos);
            fic = cf.buildForgetInvocationsCommand(keys, ids);
            // we fill the array to release keys for GC, but don't have to zero ids
            Arrays.fill(this.keys[segment], 0, lengths[segment], null);
            lengths[segment] = 0;
         }
         futures.add(forgetInvocations(segment, fic, rpcManager.getDefaultRpcOptions(true, DeliverOrder.NONE)));
      }
      CompletableFuture.allOf(futures.toArray(new CompletableFuture[this.keys.length])).join();
   }

   private CompletableFuture<?> forgetInvocations(int segment, ForgetInvocationsCommand fic, RpcOptions forgetRpcOptions) {
      DistributionInfo info = dm.getCacheTopology().getDistributionForSegment(segment);
      CompletableFuture<?> rpcFuture = rpcManager.invokeRemotelyAsync(info.writeOwners(), fic, forgetRpcOptions);
      if (info.isWriteOwner()) {
         fic.init(dataContainer, persistenceManager, this);
         fic.setOrigin(rpcManager.getAddress());
         if (trace) {
            log.tracef("Invoking %s", fic);
         }
         fic.invoke();
         if (trace) {
            Set<Object> keys = new HashSet<>(Arrays.asList(fic.keys()));
            log.tracef("Invalidation completed: " + StreamSupport.stream(dataContainer.spliterator(), false)
                  .filter(ice -> keys.contains(ice.getKey())).map(String::valueOf).collect(Collectors.joining()));
         }
      }
      return rpcFuture;
   }

}

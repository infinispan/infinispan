package org.infinispan.interceptors.distribution;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.BiConsumer;
import java.util.stream.Stream;

import org.infinispan.commands.CommandInvocationId;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.remote.RevokeBiasCommand;
import org.infinispan.commands.write.DataWriteCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.remoting.inboundhandler.DeliverOrder;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.impl.MapResponseCollector;
import org.infinispan.scattered.BiasManager;
import org.infinispan.util.concurrent.CommandAckCollector;
import org.infinispan.util.concurrent.CompletableFutures;

public class BiasedScatteredDistributionInterceptor extends ScatteredDistributionInterceptor {
   private CommandAckCollector commandAckCollector;
   private BiasManager biasManager;

   @Inject
   public void inject(CommandAckCollector collector, BiasManager biasManager) {
      this.commandAckCollector = collector;
      this.biasManager = biasManager;
   }

   @Override
   protected CompletionStage<Map<Address, Response>> singleWriteOnRemotePrimary(Address target, DataWriteCommand command) {
      Collector<Response> collector = commandAckCollector.createBiased(command.getCommandInvocationId().getId(),
            target, command.getTopologyId());
      rpcManager.sendTo(target, command, DeliverOrder.NONE);
      return collector.getFuture().thenApply(value -> Collections.singletonMap(target, value));
   }

   @Override
   protected CompletionStage<Map<Address, Response>> manyWriteOnRemotePrimary(Address target, WriteCommand command) {
      Collector<Response> collector = commandAckCollector.createMultiTargetCollector(command.getCommandInvocationId().getId(), target, command.getTopologyId());
      rpcManager.sendTo(target, command, DeliverOrder.NONE);
      return collector.getFuture().thenApply(value -> Collections.singletonMap(target, value));
   }

   @Override
   protected CompletionStage<?> completeSingleWriteOnPrimaryOriginator(DataWriteCommand command, Address backup, CompletionStage<?> rpcFuture) {
      CompletionStage<?> revokeFuture = revokeBiasSync(command.getKey(), backup, command.getTopologyId());
      return CompletableFuture.allOf(rpcFuture.toCompletableFuture(), revokeFuture.toCompletableFuture());
   }

   private CompletionStage<?> revokeBiasSync(Object key, Address backup, int topologyId) {
      BiasManager.Revocation revocation = biasManager.startRevokingRemoteBias(key, backup);
      if (revocation == null) {
         // No need to revoke from anyone
         return CompletableFutures.completedNull();
      }
      if (revocation.shouldRevoke()) {
         CompletionStage<Map<Address, Response>>
            revocationRequest = sendRevokeBias(revocation.biased(), Collections.singleton(key), topologyId, null);
         revocationRequest.whenComplete(revocation);
         return revocationRequest;
      } else {
         // there's other ongoing revocation, wait until it completes and retry (possibly having to revoke as well)
         return revocation.handleCompose(() -> revokeBiasSync(key, backup, topologyId));
      }
   }

   @Override
   protected void completeManyWriteOnPrimaryOriginator(WriteCommand command, Address backup, CountDownCompletableFuture future) {
      revokeManyKeys(backup, future, command.getAffectedKeys(), command.getTopologyId());
   }

   private void revokeManyKeys(Address backup, CountDownCompletableFuture future, Collection<?> keys, int topologyId) {
      Map<Address, GroupRevocation> revokedKeys = new HashMap<>();
      for (Object key : keys) {
         BiasManager.Revocation revocation = biasManager.startRevokingRemoteBias(key, backup);
         if (revocation != null) {
            if (revocation.shouldRevoke()) {
               for (Address member : revocation.biased()) {
                  revokedKeys.computeIfAbsent(member, m -> new GroupRevocation()).add(key, revocation);
               }
            } else {
               future.increment();
               revocation.handleCompose(() -> {
                  CompletionStage<?> revokeFuture = revokeBiasSync(key, backup, topologyId);
                  revokeFuture.thenRun(future::countDown);
                  return CompletableFutures.completedNull();
               });
            }
         }
      }
      for (Map.Entry<Address, GroupRevocation> mapping : revokedKeys.entrySet()) {
         future.increment();
         sendRevokeBias(Collections.singleton(mapping.getKey()), mapping.getValue().keys, topologyId, null).whenComplete((nil, throwable) -> {
            if (throwable != null) {
               mapping.getValue().revocations.forEach(BiasManager.Revocation::fail);
               future.completeExceptionally(throwable);
            } else {
               mapping.getValue().revocations.forEach(BiasManager.Revocation::complete);
               future.countDown();
            }
         });
      }
   }

   private static class GroupRevocation implements BiConsumer<Object, Throwable> {
      final Collection<Object> keys = new ArrayList<>();
      final Collection<BiasManager.Revocation> revocations = new ArrayList<>();

      public void add(Object key, BiasManager.Revocation revocation) {
         keys.add(key);
         revocations.add(revocation);
      }

      @Override
      public void accept(Object o, Throwable throwable) {
         if (throwable != null) {
            revocations.forEach(BiasManager.Revocation::fail);
         } else {
            revocations.forEach(BiasManager.Revocation::complete);
         }
      }
   }

   @Override
   protected void singleWriteResponse(InvocationContext ctx, DataWriteCommand cmd, Object returnValue) {
      BiasManager.Revocation revocation;
      if (!cmd.isSuccessful() || (revocation = biasManager.startRevokingRemoteBias(cmd.getKey(), ctx.getOrigin())) == null) {
         rpcManager.sendTo(ctx.getOrigin(), cf.buildPrimaryAckCommand(cmd.getCommandInvocationId().getId(),
               cmd.isSuccessful(), returnValue, null), DeliverOrder.NONE);
      } else if (revocation.shouldRevoke()) {
         Address[] waitFor = revocation.biased().toArray(new Address[revocation.biased().size()]);
         sendRevokeBias(revocation.biased(), Collections.singleton(cmd.getKey()), cmd.getTopologyId(), cmd.getCommandInvocationId())
               .whenComplete(revocation);
         // When the revocation does not succeed this node does not care; originator will get timeout
         // expecting BackupAckCommand from the node that had the bias revoked
         rpcManager.sendTo(ctx.getOrigin(), cf.buildPrimaryAckCommand(cmd.getCommandInvocationId().getId(),
               cmd.isSuccessful(), returnValue, waitFor), DeliverOrder.NONE);
      } else {
         // We'll send the response & revocations later but the command
         // will be already completed on this node
         revocation.handleCompose(() -> {
            singleWriteResponse(ctx, cmd, returnValue);
            return null;
         });
      }
   }

   @Override
   protected void manyWriteResponse(InvocationContext ctx, WriteCommand cmd, Object returnValue) {
      List<Address> waitFor = null;
      if (cmd.isSuccessful()) {
         Collection<?> keys = cmd.getAffectedKeys();
         Map<Address, GroupRevocation> revocations = new HashMap<>();
         List<CompletableFuture<List<Address>>> futures = null;
         for (Object key : keys) {
            BiasManager.Revocation revocation = biasManager.startRevokingRemoteBias(key, ctx.getOrigin());
            if (revocation == null) {
               continue;
            } else if (revocation.shouldRevoke()) {
               if (waitFor == null)
                  waitFor = new ArrayList<>(keys.size());
               waitFor.addAll(revocation.biased());
               for (Address b : revocation.biased()) {
                  revocations.computeIfAbsent(b, a -> new GroupRevocation()).add(key, revocation);
               }
            } else {
               if (futures == null)
                  futures = new ArrayList<>();
               futures.add(revocation.handleCompose(() -> revokeBiasAsync(cmd, key, ctx.getOrigin())));
            }
         }
         for (Map.Entry<Address, GroupRevocation> pair : revocations.entrySet()) {
            sendRevokeBias(Collections.singleton(pair.getKey()), pair.getValue().keys, cmd.getTopologyId(), cmd.getCommandInvocationId())
                  .whenComplete(pair.getValue());
         }
         if (futures != null) {
            List<Address> waitForFinal = waitFor;
            CompletableFuture<List<Address>>[] cfs = futures.toArray(new CompletableFuture[futures.size()]);
            CompletableFuture.allOf(cfs).thenRun(() -> {
               Address[] waitForAddresses = null;
               if (waitForFinal != null) {
                  Stream.of(cfs).map(CompletableFuture::join).filter(Objects::nonNull).forEach(waitForFinal::addAll);
                  waitForAddresses = waitForFinal.toArray(new Address[waitForFinal.size()]);
               }
               rpcManager.sendTo(ctx.getOrigin(), cf.buildPrimaryAckCommand(cmd.getCommandInvocationId().getId(),
                     cmd.isSuccessful(), returnValue, waitForAddresses), DeliverOrder.NONE);
            });
            return;
         }
      }
      Address[] waitForAddresses = waitFor == null ? null : waitFor.toArray(new Address[waitFor.size()]);
      rpcManager.sendTo(ctx.getOrigin(), cf.buildPrimaryAckCommand(cmd.getCommandInvocationId().getId(),
            cmd.isSuccessful(), returnValue, waitForAddresses), DeliverOrder.NONE);
   }

   private CompletableFuture<List<Address>> revokeBiasAsync(WriteCommand cmd, Object key, Address backup) {
      BiasManager.Revocation revocation = biasManager.startRevokingRemoteBias(key, backup);
      if (revocation == null) {
         return CompletableFutures.completedNull();
      } else if (revocation.shouldRevoke()) {
         sendRevokeBias(revocation.biased(), Collections.singleton(key), cmd.getTopologyId(), cmd.getCommandInvocationId())
               .whenComplete(revocation);
         return CompletableFuture.completedFuture(revocation.biased());
      } else {
         return revocation.handleCompose(() -> revokeBiasAsync(cmd, key, backup));
      }
   }

   private CompletionStage<Map<Address, Response>> sendRevokeBias(Collection<Address> targets, Collection<Object> keys, int topologyId, CommandInvocationId commandInvocationId) {
      try {
         Address ackTarget = null;
         long id = 0;
         if (commandInvocationId != null) {
            ackTarget = commandInvocationId.getAddress();
            id = commandInvocationId.getId();
         }
         RevokeBiasCommand revokeBiasCommand = cf.buildRevokeBiasCommand(ackTarget, id, topologyId, keys);
         return rpcManager.invokeCommand(targets, revokeBiasCommand, MapResponseCollector.ignoreLeavers(targets.size()), rpcManager.getSyncRpcOptions());
      } catch (Throwable t) {
         return CompletableFutures.completedExceptionFuture(t);
      }
   }

   @Override
   protected void handleClear(InvocationContext ctx, VisitableCommand command, Object ignored) {
      super.handleClear(ctx, command, ignored);
      biasManager.clear();
   }
}

package org.infinispan.query.affinity;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.infinispan.commands.remote.BaseRpcCommand;
import org.infinispan.statetransfer.StateRequestCommand;
import org.infinispan.statetransfer.StateResponseCommand;

class RpcCollector {

   private List<RpcDetail> rpcCollection = new ArrayList<>();

   private final List<Class<? extends BaseRpcCommand>> stateTransferCommands = Arrays.asList(StateRequestCommand.class, StateResponseCommand.class);

   private Predicate<RpcDetail> stateTransferPredicate = rpc -> stateTransferCommands.contains(rpc.getCommand().getClass());

   private Predicate<RpcDetail> sameDestinationPredicate = RpcDetail::isRpcToItself;

   private Predicate<RpcDetail> cacheNamePredicate(String cacheName) {
      return rpc -> rpc.getCacheName().equals(cacheName);
   }

   synchronized void addRPC(RpcDetail rpcDetail) {
      rpcCollection.add(rpcDetail);
   }

   synchronized Set<RpcDetail> getRpcsForCache(String cacheName) {
      return rpcCollection.stream()
            .filter(sameDestinationPredicate.negate())
            .filter(stateTransferPredicate.negate())
            .filter(cacheNamePredicate(cacheName))
            .collect(Collectors.toSet());
   }

}

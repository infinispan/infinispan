package org.infinispan.interceptors.distribution;

import java.util.Collection;
import java.util.function.Function;

import org.infinispan.commands.write.WriteCommand;
import org.infinispan.commons.util.IntSet;
import org.infinispan.distribution.LocalizedCacheTopology;
import org.infinispan.interceptors.InvocationSuccessFunction;
import org.infinispan.remoting.transport.Address;

public abstract class WriteManyCommandHelper<C extends WriteCommand, Container, Item> {
   protected final InvocationSuccessFunction<C> remoteCallback;

   protected WriteManyCommandHelper(Function<WriteManyCommandHelper<C, ?, ?>, InvocationSuccessFunction<C>> createRemoteCallback) {
      this.remoteCallback = createRemoteCallback.apply(this);
   }

   public InvocationSuccessFunction<C> getRemoteCallback() {
      return remoteCallback;
   }

   public abstract C copyForLocal(C cmd, Container container);

   public abstract C copyForPrimary(C cmd, LocalizedCacheTopology topology, IntSet segments);

   public abstract C copyForBackup(C cmd, LocalizedCacheTopology topology, Address target, IntSet segments);

   public abstract Collection<Item> getItems(C cmd);

   public abstract Object item2key(Item item);

   public abstract Container newContainer();

   public abstract void accumulate(Container container, Item item);

   public abstract int containerSize(Container container);

   public abstract Iterable<Object> toKeys(Container container);

   public abstract boolean shouldRegisterRemoteCallback(C cmd);

   public abstract Object transformResult(Object[] results);
}

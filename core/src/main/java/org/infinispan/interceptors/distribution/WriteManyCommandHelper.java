package org.infinispan.interceptors.distribution;

import java.util.Collection;
import java.util.Set;
import java.util.function.Function;

import org.infinispan.commands.write.WriteCommand;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.interceptors.InvocationSuccessFunction;

abstract class WriteManyCommandHelper<C extends WriteCommand, Container, Item> {
   protected final InvocationSuccessFunction remoteCallback;

   protected WriteManyCommandHelper(Function<WriteManyCommandHelper<C, ?, ?>, InvocationSuccessFunction> createRemoteCallback) {
      this.remoteCallback = createRemoteCallback.apply(this);
   }

   public abstract C copyForLocal(C cmd, Container container);

   public abstract C copyForPrimary(C cmd, ConsistentHash ch, Set<Integer> segments);

   public abstract C copyForBackup(C cmd, ConsistentHash ch, Set<Integer> segments);

   public abstract Collection<Item> getItems(C cmd);

   public abstract Object item2key(Item item);

   public abstract Container newContainer();

   public abstract void accumulate(Container container, Item item);

   public abstract int containerSize(Container container);

   public abstract Iterable<Object> toKeys(Container container);

   public abstract boolean isForwarded(C cmd);

   public abstract void setForwarded(C backupCommand);

   public abstract Object transformResult(Object[] results);

   public abstract Collection<Item> asCollection(Container container);
}

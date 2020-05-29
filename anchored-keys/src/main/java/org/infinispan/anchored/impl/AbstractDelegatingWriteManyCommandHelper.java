package org.infinispan.anchored.impl;

import java.util.Collection;

import org.infinispan.commands.write.WriteCommand;
import org.infinispan.commons.util.IntSet;
import org.infinispan.distribution.LocalizedCacheTopology;
import org.infinispan.interceptors.distribution.WriteManyCommandHelper;
import org.infinispan.remoting.transport.Address;

public abstract class AbstractDelegatingWriteManyCommandHelper<C extends WriteCommand, Item, Container> extends
                                                                                                        WriteManyCommandHelper<C, Item, Container> {
   protected final WriteManyCommandHelper<C, Item, Container> helper;

   public AbstractDelegatingWriteManyCommandHelper(WriteManyCommandHelper<C, Item, Container> helper) {
      super(h -> helper.getRemoteCallback());
      this.helper = helper;
   }

   @Override
   public C copyForLocal(C cmd, Item item) {
      return helper.copyForLocal(cmd, item);
   }

   @Override
   public C copyForPrimary(C cmd, LocalizedCacheTopology topology, IntSet segments) {
      return helper.copyForPrimary(cmd, topology, segments);
   }

   @Override
   public C copyForBackup(C cmd, LocalizedCacheTopology topology, Address target,
                          IntSet segments) {
      return helper.copyForBackup(cmd, topology, target, segments);
   }

   @Override
   public Collection<Container> getItems(C cmd) {
      return helper.getItems(cmd);
   }

   @Override
   public Object item2key(Container container) {
      return helper.item2key(container);
   }

   @Override
   public Item newContainer() {
      return helper.newContainer();
   }

   @Override
   public void accumulate(Item item, Container container) {
      helper.accumulate(item, container);
   }

   @Override
   public int containerSize(Item item) {
      return helper.containerSize(item);
   }

   @Override
   public Iterable<Object> toKeys(Item item) {
      return helper.toKeys(item);
   }

   @Override
   public boolean shouldRegisterRemoteCallback(C cmd) {
      return helper.shouldRegisterRemoteCallback(cmd);
   }

   @Override
   public Object transformResult(Object[] results) {
      return helper.transformResult(results);
   }
}

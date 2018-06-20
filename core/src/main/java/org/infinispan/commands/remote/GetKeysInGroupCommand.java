package org.infinispan.commands.remote;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.infinispan.commands.AbstractTopologyAffectedCommand;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.Visitor;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.distribution.group.impl.GroupManager;

/**
 * {@link org.infinispan.commands.VisitableCommand} that fetches the keys belonging to a group.
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
public class GetKeysInGroupCommand extends AbstractTopologyAffectedCommand implements VisitableCommand {

   public static final byte COMMAND_ID = 43;

   private Object groupName;
   /*
   local state to avoid checking everywhere if the node in which this command is executed is the group owner.
    */
   private transient boolean isGroupOwner;
   private transient GroupManager groupManager;

   public GetKeysInGroupCommand(long flagsBitSet, Object groupName) {
      this.groupName = groupName;
      setFlagsBitSet(flagsBitSet);
   }

   public GetKeysInGroupCommand() {
   }

   public GetKeysInGroupCommand setGroupManager(GroupManager groupManager) {
      this.groupManager = groupManager;
      return this;
   }

   @Override
   public Object perform(InvocationContext ctx) throws Throwable {
      final KeyValueCollector collector = ctx.isOriginLocal() ?
            new LocalContextKeyValueCollector() :
            new RemoteContextKeyValueCollector();
      ctx.forEachEntry((key, entry) -> {
         if (getGroupName().equals(groupManager.getGroup(key)) && !entry.isRemoved() && !entry.isNull()) {
            collector.addCacheEntry(entry);
         }
      });
      return collector.getResult();
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public void writeTo(ObjectOutput output) throws IOException {
      output.writeObject(groupName);
      output.writeLong(FlagBitSets.copyWithoutRemotableFlags(getFlagsBitSet()));
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      groupName = input.readObject();
      setFlagsBitSet(input.readLong());
   }

   @Override
   public boolean isReturnValueExpected() {
      return true;
   }

   @Override
   public boolean canBlock() {
      return false;
   }

   @Override
   public Object acceptVisitor(InvocationContext ctx, Visitor visitor) throws Throwable {
      return visitor.visitGetKeysInGroupCommand(ctx, this);
   }

   @Override
   public LoadType loadType() {
      return LoadType.OWNER;
   }

   public Object getGroupName() {
      return groupName;
   }

   @Override
   public String toString() {
      return "GetKeysInGroupCommand{" +
            "groupName='" + groupName + '\'' +
            ", flags=" + printFlags() +
            '}';
   }

   public boolean isGroupOwner() {
      return isGroupOwner;
   }

   public void setGroupOwner(boolean isGroupOwner) {
      this.isGroupOwner = isGroupOwner;
   }

   private interface KeyValueCollector {
      void addCacheEntry(CacheEntry entry);

      Object getResult();
   }

   private static class LocalContextKeyValueCollector implements KeyValueCollector {

      private final Map<Object, Object> map;

      private LocalContextKeyValueCollector() {
         map = new HashMap<>();
      }

      @Override
      public void addCacheEntry(CacheEntry entry) {
         map.put(entry.getKey(), entry.getValue());
      }

      @Override
      public Object getResult() {
         return map;
      }
   }

   private static class RemoteContextKeyValueCollector implements KeyValueCollector {

      private final List<CacheEntry> list;

      private RemoteContextKeyValueCollector() {
         list = new LinkedList<>();
      }

      @Override
      public void addCacheEntry(CacheEntry entry) {
         list.add(entry);
      }

      @Override
      public Object getResult() {
         return list;
      }
   }
}

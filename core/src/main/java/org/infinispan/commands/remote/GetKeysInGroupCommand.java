package org.infinispan.commands.remote;

import org.infinispan.commands.AbstractFlagAffectedCommand;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.Visitor;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.distribution.group.GroupFilter;
import org.infinispan.distribution.group.GroupManager;
import org.infinispan.lifecycle.ComponentStatus;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * {@link org.infinispan.commands.VisitableCommand} that fetches the keys belonging to a group.
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
public class GetKeysInGroupCommand extends AbstractFlagAffectedCommand implements VisitableCommand {

   public static final byte COMMAND_ID = 43;

   private String groupName;
   /*
   local state to avoid checking everywhere if the node in which this command is executed is the group owner.
    */
   private transient boolean isGroupOwner;
   private transient GroupManager groupManager;

   public GetKeysInGroupCommand(Set<Flag> flags, String groupName) {
      this.groupName = groupName;
      setFlags(flags);
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
      final GroupFilter<Object> filter = new GroupFilter<>(getGroupName(), groupManager);
      for (CacheEntry entry : ctx.getLookedUpEntries().values()) {
         if (!entry.isRemoved() && filter.accept(entry.getKey())) {
            collector.addCacheEntry(entry);
         }
      }
      return collector.getResult();
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public Object[] getParameters() {
      return new Object[]{groupName, flags};
   }

   @Override
   public void setParameters(int commandId, Object[] parameters) {
      if (commandId != COMMAND_ID) {
         throw new IllegalArgumentException("Wrong command id");
      }
      this.groupName = (String) parameters[0];
      //noinspection unchecked
      this.flags = (Set<Flag>) parameters[1];
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
   public boolean shouldInvoke(InvocationContext ctx) {
      return true;
   }

   @Override
   public boolean ignoreCommandOnStatus(ComponentStatus status) {
      return false;
   }

   public String getGroupName() {
      return groupName;
   }

   @Override
   public String toString() {
      return "GetKeysInGroupCommand{" +
            "groupName='" + groupName + '\'' +
            '}';
   }

   public boolean isGroupOwner() {
      return isGroupOwner;
   }

   public void setGroupOwner(boolean isGroupOwner) {
      this.isGroupOwner = isGroupOwner;
   }

   private static interface KeyValueCollector {
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

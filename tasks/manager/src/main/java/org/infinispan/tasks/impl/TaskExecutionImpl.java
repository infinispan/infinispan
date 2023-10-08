package org.infinispan.tasks.impl;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.commons.util.Util;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.tasks.TaskContext;
import org.infinispan.tasks.TaskExecution;

/**
 * TaskExecutionImpl. A concrete representation of a {@link TaskExecution}
 *
 * @author Tristan Tarrant
 * @since 8.1
 */
@ProtoTypeId(ProtoStreamTypeIds.TASK_EXECUTION_IMPL)
public class TaskExecutionImpl implements TaskExecution {
   @ProtoField(1)
   final UUID uuid;

   @ProtoField(2)
   final String name;

   @ProtoField(3)
   final String what;

   @ProtoField(4)
   final String where;

   @ProtoField(5)
   final String who;

   Instant start;

   @ProtoFactory
   TaskExecutionImpl(UUID uuid, String name, String what, String where, String who) {
      this.uuid = uuid;
      this.name = name;
      this.what = what;
      this.where = where;
      this.who = who;
   }

   public TaskExecutionImpl(String name, String where, String who, TaskContext context) {
      this.uuid = Util.threadLocalRandomUUID();
      this.name = name;
      this.where = where;
      this.who = who;
      this.what = context.getCache().isPresent() ? context.getCache().get().getName() : null;
   }

   public UUID getUUID() {
      return uuid;
   }

   @Override
   public String getName() {
      return name;
   }

   @Override
   public Instant getStart() {
      return start;
   }

   @Override
   public Optional<String> getWhat() {
      return Optional.ofNullable(what);
   }

   @Override
   public String getWhere() {
      return where;
   }

   @Override
   public Optional<String> getWho() {
      return Optional.ofNullable(who);
   }

   public void setStart(Instant start) {
      this.start = start;
   }
}

package org.infinispan.tasks.impl;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

import org.infinispan.commons.marshall.SerializeWith;
import org.infinispan.commons.util.Util;
import org.infinispan.tasks.TaskContext;
import org.infinispan.tasks.TaskExecution;

/**
 * TaskExecutionImpl. A concrete representation of a {@link TaskExecution}
 *
 * @author Tristan Tarrant
 * @since 8.1
 */
@SerializeWith(TaskExecutionImplExternalizer.class)
public class TaskExecutionImpl implements TaskExecution {
   final UUID uuid;
   final String name;
   final Optional<String> what;
   final String where;
   final Optional<String> who;
   Instant start;

   TaskExecutionImpl(UUID uuid, String name, Optional<String> what, String where, Optional<String> who) {
      this.uuid = uuid;
      this.name = name;
      this.what = what;
      this.where = where;
      this.who = who;
   }

   TaskExecutionImpl(String name, Optional<String> what, String where, Optional<String> who) {
      this(Util.threadLocalRandomUUID(), name, what, where, who);
   }

   public TaskExecutionImpl(String name, String where, Optional<String> who, TaskContext context) {
      this.uuid = Util.threadLocalRandomUUID();
      this.name = name;
      this.what = context.getCache().map(cache -> cache.getName());
      this.where = where;
      this.who = who;
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
      return what;
   }

   @Override
   public String getWhere() {
      return where;
   }

   @Override
   public Optional<String> getWho() {
      return who;
   }

   public void setStart(Instant start) {
      this.start = start;
   }
}

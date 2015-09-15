package org.infinispan.tasks.impl.history;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

import org.infinispan.commons.marshall.SerializeWith;
import org.infinispan.tasks.TaskContext;
import org.infinispan.tasks.TaskEvent;
import org.infinispan.tasks.TaskEventStatus;

/**
 * TaskEventImpl. A concrete representation of a task event
 *
 * @author Tristan Tarrant
 * @since 8.1
 */
@SerializeWith(TaskEventImplExternalizer.class)
public class TaskEventImpl implements TaskEvent {
   final UUID uuid;
   final String name;
   final Optional<String> what;
   final String where;
   final Optional<String> who;

   Instant finish;
   Optional<String> log;
   Instant start;
   TaskEventStatus status = TaskEventStatus.PENDING;

   TaskEventImpl(UUID uuid, String name, Optional<String> what, String where, Optional<String> who) {
      this.uuid = uuid;
      this.name = name;
      this.what = what;
      this.where = where;
      this.who = who;
   }

   TaskEventImpl(String name, Optional<String> what, String where, Optional<String> who) {
      this(UUID.randomUUID(), name, what, where, who);
   }

   public TaskEventImpl(String name, String where, TaskContext context) {
      this.uuid = UUID.randomUUID();
      this.name = name;
      this.what = context.getCache().map(cache -> cache.getName());
      this.where = where;
      this.who = Optional.empty(); // TODO
   }

   public UUID getUUID() {
      return uuid;
   }

   @Override
   public Instant getFinish() {
      return finish;
   }

   public Optional<String> getLog() {
      return log;
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
   public TaskEventStatus getStatus() {
      return status;
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

   public void setFinish(Instant finish) {
      this.finish = finish;
   }

   public void setLog(Optional<String> log) {
      this.log = log;
   }

   public void setStart(Instant start) {
      this.start = start;
   }

   public void setStatus(TaskEventStatus status) {
      this.status = status;
   }

}

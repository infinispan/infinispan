package org.infinispan.server.eventlogger;

import java.time.Instant;
import java.util.Optional;

import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.util.logging.events.EventLog;
import org.infinispan.util.logging.events.EventLogCategory;
import org.infinispan.util.logging.events.EventLogLevel;

/**
 * ServerEvent.
 *
 * @author Tristan Tarrant
 * @since 8.2
 */
public class ServerEventImpl implements EventLog {

   private Instant when;

   @ProtoField(number = 1)
   EventLogLevel level;

   @ProtoField(number = 2)
   EventLogCategory category;

   @ProtoField(number = 3)
   String message;

   @ProtoField(number = 4, name = "epoch", defaultValue = "0")
   long getEpoch() {
      return when.getEpochSecond();
   }

   @ProtoField(number = 5, name = "detail")
   String detail;


   @ProtoField(number = 6, name = "who")
   String who;

   @ProtoField(number = 7, name = "context")
   String context;

   @ProtoField(number = 8, name = "scope")
   String scope;

   ServerEventImpl() {}

   ServerEventImpl(EventLogLevel level, EventLogCategory category, Instant when, String message, String detail, String context, String who, String scope) {
      this.level = level;
      this.category = category;
      this.message = message;
      this.when = when;
      this.detail = detail;
      this.context = context;
      this.who = who;
      this.scope = scope;
   }

   ServerEventImpl(EventLogLevel level, EventLogCategory category, Instant when, String message) {
      this(level, category, when, message, null, null, null, null);
   }

   @Override
   public Instant getWhen() {
      return when;
   }

   @Override
   public EventLogLevel getLevel() {
      return level;
   }

   @Override
   public EventLogCategory getCategory() {
      return category;
   }

   @Override
   public String getMessage() {
      return message;
   }

   @Override
   public Optional<String> getDetail() {
      return Optional.ofNullable(detail);
   }

   @Override
   public Optional<String> getWho() {
      return Optional.ofNullable(who);
   }

   @Override
   public Optional<String> getContext() {
      return Optional.ofNullable(context);
   }

   @Override
   public Optional<String> getScope() {
      return Optional.ofNullable(scope);
   }

   void setEpoch(long epoch) {
      this.when = Instant.ofEpochSecond(epoch);
   }

   @Override
   public int compareTo(EventLog that) {
      // Intentionally backwards
      return that.getWhen().compareTo(this.when);
   }

   @Override
   public String toString() {
      return "ServerEventImpl{" +
            "level=" + level +
            ", category=" + category +
            ", when=" + when +
            ", message='" + message + '\'' +
            ", detail=" + detail +
            ", context=" + context +
            ", who=" + who +
            ", scope=" + scope +
            '}';
   }
}

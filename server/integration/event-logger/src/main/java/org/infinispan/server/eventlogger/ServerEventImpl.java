package org.infinispan.server.eventlogger;

import java.time.Instant;
import java.util.Optional;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.util.logging.events.EventLog;
import org.infinispan.util.logging.events.EventLogCategory;
import org.infinispan.util.logging.events.EventLogLevel;

/**
 * ServerEvent.
 *
 * @author Tristan Tarrant
 * @since 8.2
 */
@ProtoTypeId(ProtoStreamTypeIds.SERVER_EVENT_IMPL)
public final class ServerEventImpl implements EventLog {

   private final Instant when;

   private final EventLogLevel level;

   private final EventLogCategory category;

   private final String message;

   private final String detail;

   private final String who;

   private final String context;

   private final String scope;

   @ProtoFactory
   ServerEventImpl(long whenMs, EventLogLevel level, EventLogCategory category, String message, String detail, String context, String who, String scope) {
      this(Instant.ofEpochMilli(whenMs), level, category, message, detail, context, who, scope);
   }

   ServerEventImpl(Instant when, EventLogLevel level, EventLogCategory category, String message, String detail, String context, String who, String scope) {
      this.level = level;
      this.category = category;
      this.message = message;
      this.when = when;
      this.detail = detail;
      this.context = context;
      this.who = who;
      this.scope = scope;
   }

   ServerEventImpl(Instant when, EventLogLevel level, EventLogCategory category, String message) {
      this(when, level, category, message, null, null, null, null);
   }

   @Override
   public Instant getWhen() {
      return when;
   }

   /**
    * Milliseconds since epoch.
    */
   @ProtoField(number = 1, name = "when", defaultValue = "0")
   long getWhenMs() {
      return when.toEpochMilli();
   }

   @ProtoField(number = 2)
   @Override
   public EventLogLevel getLevel() {
      return level;
   }

   @ProtoField(number = 3)
   @Override
   public EventLogCategory getCategory() {
      return category;
   }

   @ProtoField(number = 4)
   @Override
   public String getMessage() {
      return message;
   }

   @ProtoField(number = 5)
   @Override
   public Optional<String> getDetail() {
      return Optional.ofNullable(detail);
   }

   @ProtoField(number = 6)
   @Override
   public Optional<String> getWho() {
      return Optional.ofNullable(who);
   }

   @ProtoField(number = 7)
   @Override
   public Optional<String> getContext() {
      return Optional.ofNullable(context);
   }

   @ProtoField(number = 8)
   @Override
   public Optional<String> getScope() {
      return Optional.ofNullable(scope);
   }

   @Override
   public int compareTo(EventLog that) {
      // Intentionally backwards
      return that.getWhen().compareTo(this.when);
   }

   @Override
   public String toString() {
      return "ServerEventImpl{" +
            "when=" + when +
            ", level=" + level +
            ", category=" + category +
            ", message='" + message + '\'' +
            ", detail=" + detail +
            ", context=" + context +
            ", who=" + who +
            ", scope=" + scope +
            '}';
   }
}

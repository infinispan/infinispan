package org.infinispan.server.logging.events;

import java.time.Instant;
import java.util.Optional;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.util.logging.events.EventLogCategory;
import org.infinispan.util.logging.events.EventLogLevel;
import org.infinispan.util.logging.events.impl.BaseEventLog;

/**
 * ServerEvent.
 *
 * @author Tristan Tarrant
 * @since 8.2
 */
@ProtoTypeId(ProtoStreamTypeIds.SERVER_EVENT_IMPL)
public final class ServerEventImpl extends BaseEventLog {
   @ProtoFactory
   ServerEventImpl(long whenMs, EventLogLevel level, EventLogCategory category, String message, String detail, String context, String who, String scope) {
      this(Instant.ofEpochMilli(whenMs), level, category, message, detail, context, who, scope);
   }

   ServerEventImpl(Instant when, EventLogLevel level, EventLogCategory category, String message, String detail, String context, String who, String scope) {
      super(when, level, category, message, detail, context, who, scope);
   }

   ServerEventImpl(Instant when, EventLogLevel level, EventLogCategory category, String message) {
      this(when, level, category, message, null, null, null, null);
   }

   /**
    * Milliseconds since epoch.
    */
   @ProtoField(number = 1, name = "when", defaultValue = "0")
   long getWhenMs() {
      return when.toEpochMilli();
   }

   @ProtoField(2)
   @Override
   public EventLogLevel getLevel() {
      return level;
   }

   @ProtoField(3)
   @Override
   public EventLogCategory getCategory() {
      return category;
   }

   @ProtoField(4)
   @Override
   public String getMessage() {
      return message;
   }

   @ProtoField(5)
   @Override
   public Optional<String> getDetail() {
      return Optional.ofNullable(detail);
   }

   @ProtoField(6)
   @Override
   public Optional<String> getWho() {
      return Optional.ofNullable(who);
   }

   @ProtoField(7)
   @Override
   public Optional<String> getContext() {
      return Optional.ofNullable(context);
   }

   @ProtoField(8)
   @Override
   public Optional<String> getScope() {
      return Optional.ofNullable(scope);
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

package org.infinispan.util.logging.events.impl;

import java.time.Instant;
import java.util.Optional;

import net.jcip.annotations.Immutable;
import org.infinispan.util.logging.events.EventLog;
import org.infinispan.util.logging.events.EventLogCategory;
import org.infinispan.util.logging.events.EventLogLevel;

@Immutable
public class BaseEventLog implements EventLog {
   protected final Instant when;
   protected final EventLogLevel level;
   protected final EventLogCategory category;
   protected final String message;
   protected final String detail;
   protected final String who;
   protected final String context;
   protected final String scope;

   public BaseEventLog(Instant when, EventLogLevel level, EventLogCategory category, String message, String detail,
                       String context, String who, String scope) {
      this.when = when;
      this.level = level;
      this.category = category;
      this.message = message;
      this.detail = detail;
      this.who = who;
      this.context = context;
      this.scope = scope;
   }

   public BaseEventLog(Instant when, EventLogLevel level, EventLogCategory category, String message) {
      this(when, level, category, message, null, null, null, null);
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
   public String getMessage() {
      return message;
   }

   @Override
   public EventLogCategory getCategory() {
      return category;
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

   @Override
   public int compareTo(EventLog that) {
      // Intentionally backwards
      return that.getWhen().compareTo(this.when);
   }

   @Override public String toString() {
      return "BaseEventLog{" +
            "when=" + when +
            ", level=" + level +
            ", category=" + category +
            ", message='" + message + '\'' +
            ", detail='" + detail + '\'' +
            ", who='" + who + '\'' +
            ", context='" + context + '\'' +
            ", scope='" + scope + '\'' +
            '}';
   }
}

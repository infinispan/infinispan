package org.infinispan.server.eventlogger;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.time.Instant;
import java.util.Optional;
import java.util.Set;

import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.marshall.SerializeWith;
import org.infinispan.commons.util.Util;
import org.infinispan.util.logging.events.EventLog;
import org.infinispan.util.logging.events.EventLogCategory;
import org.infinispan.util.logging.events.EventLogLevel;

/**
 * ServerEvent.
 *
 * @author Tristan Tarrant
 * @since 8.2
 */
@SerializeWith(ServerEventImpl.Externalizer.class)
public class ServerEventImpl implements EventLog {

   private final EventLogLevel level;
   private final EventLogCategory category;
   private final Instant when;
   private final String message;
   private final Optional<String> detail;
   private final Optional<String> context;
   private final Optional<String> who;
   private final Optional<String> scope;

   ServerEventImpl(EventLogLevel level, EventLogCategory category, Instant when, String message, Optional<String> detail, Optional<String> context, Optional<String> who, Optional<String> scope) {
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
      this(level, category, when, message, Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());
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
      return detail;
   }

   @Override
   public Optional<String> getWho() {
      return who;
   }

   @Override
   public Optional<String> getContext() {
      return context;
   }

   @Override
   public Optional<String> getScope() {
      return scope;
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

   @SuppressWarnings("serial")
   public static class Externalizer extends AbstractExternalizer<ServerEventImpl> {

      @Override
      public void writeObject(ObjectOutput oo, ServerEventImpl event) throws IOException {
         oo.writeObject(event.level);
         oo.writeObject(event.category);
         oo.writeObject(event.when);
         oo.writeUTF(event.message);
         oo.writeObject(event.detail);
         oo.writeObject(event.context);
         oo.writeObject(event.who);
         oo.writeObject(event.scope);
      }

      @Override
      public ServerEventImpl readObject(ObjectInput oi) throws IOException, ClassNotFoundException {
         EventLogLevel level = (EventLogLevel) oi.readObject();
         EventLogCategory category = (EventLogCategory) oi.readObject();
         Instant when = (Instant) oi.readObject();
         String message = oi.readUTF();
         Optional<String> detail = (Optional<String>) oi.readObject();
         Optional<String> context = (Optional<String>) oi.readObject();
         Optional<String> who = (Optional<String>) oi.readObject();
         Optional<String> scope = (Optional<String>) oi.readObject();
         return new ServerEventImpl(level, category, when, message, detail, context, who, scope);
      }

      @Override
      public Set<Class<? extends ServerEventImpl>> getTypeClasses() {
         return Util.<Class<? extends ServerEventImpl>> asSet(ServerEventImpl.class);
      }

   }

}

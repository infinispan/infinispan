package org.infinispan.functional.impl;

import java.io.IOException;
import java.util.Collection;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.commons.marshall.UserObjectInput;
import org.infinispan.commons.marshall.UserObjectOutput;
import org.infinispan.commons.util.Util;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.marshall.core.Ids;

/**
 * Responses for functional commands that allow to record statistics.
 */
public class StatsEnvelope<T> {
   // hit and miss are exclusive flags since the command might not read the entry at all
   public static final byte HIT = 1;
   public static final byte MISS = 2;
   public static final byte CREATE = 4;
   public static final byte UPDATE = 8;
   public static final byte DELETE = 16;

   private final T value;
   private final byte flags;

   public static <T> StatsEnvelope<T> create(T returnValue, CacheEntry<?, ?> e, boolean exists, boolean isRead) {
      byte flags = 0;
      if (isRead) {
         if (exists) flags |= HIT;
         else flags |= MISS;
      }
      if (exists) {
         if (e.getValue() == null) {
            flags |= DELETE;
         } else if (e.isChanged()) {
            flags |= UPDATE;
         }
      } else if (e.getValue() != null) {
         flags |= CREATE;
      }
      return new StatsEnvelope(returnValue, flags);
   }

   public static <R> StatsEnvelope create(R ret, boolean isNull) {
      return new StatsEnvelope(ret, isNull ? MISS : HIT);
   }


   public static Object unpack(InvocationContext ctx, VisitableCommand command, Object o) {
      return ((StatsEnvelope<?>) o).value;
   }

   public static Object unpackCollection(InvocationContext ctx, VisitableCommand command, Object o) {
      return ((Collection<StatsEnvelope<?>>) o).stream().map(StatsEnvelope::value).collect(Collectors.toList());
   }

   public static Object unpackStream(InvocationContext ctx, VisitableCommand command, Object o) {
      return ((Stream<StatsEnvelope<?>>) o).map(StatsEnvelope::value);
   }

   private StatsEnvelope(T value, byte flags) {
      this.value = value;
      this.flags = flags;
   }

   public T value() {
      return value;
   }

   public byte flags() {
      return flags;
   }

   @Override
   public String toString() {
      return "StatsEnvelope{value=" + value + ", flags="
            + ((flags & HIT) != 0 ? 'H' : '_')
            + ((flags & MISS) != 0 ? 'M' : '_')
            + ((flags & CREATE) != 0 ? 'C' : '_')
            + ((flags & UPDATE) != 0 ? 'U' : '_')
            + ((flags & DELETE) != 0 ? 'D' : '_') + "}";
   }

   public boolean isHit() {
      return (flags & HIT) != 0;
   }

   public boolean isMiss() {
      return (flags & MISS) != 0;
   }

   public boolean isDelete() {
      return (flags & DELETE) != 0;
   }

   public static class Externalizer implements AdvancedExternalizer<StatsEnvelope> {
      @Override
      public Set<Class<? extends StatsEnvelope>> getTypeClasses() {
         return Util.asSet(StatsEnvelope.class);
      }

      @Override
      public Integer getId() {
         return Ids.STATS_ENVELOPE;
      }

      @Override
      public void writeObject(UserObjectOutput output, StatsEnvelope object) throws IOException {
         output.writeObject(object.value);
         output.writeByte(object.flags);
      }

      @Override
      public StatsEnvelope readObject(UserObjectInput input) throws IOException, ClassNotFoundException {
         return new StatsEnvelope(input.readObject(), input.readByte());
      }
   }
}

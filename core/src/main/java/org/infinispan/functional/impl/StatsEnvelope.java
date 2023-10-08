package org.infinispan.functional.impl;

import java.util.Collection;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.marshall.protostream.impl.MarshallableObject;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * Responses for functional commands that allow to record statistics.
 */
@ProtoTypeId(ProtoStreamTypeIds.FUNCTIONAL_STATS_ENVELOPE)
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

   @ProtoFactory
   StatsEnvelope(MarshallableObject<T> wrappedValue, byte flags) {
      this(MarshallableObject.unwrap(wrappedValue), flags);
   }

   @ProtoField(number = 1, name = "value")
   MarshallableObject<T> getWrappedValue() {
      return MarshallableObject.create(value);
   }

   public T value() {
      return value;
   }

   @ProtoField(2)
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
}

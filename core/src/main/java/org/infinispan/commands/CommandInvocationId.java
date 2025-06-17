package org.infinispan.commands;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.remoting.transport.Address;

/**
 * Represents a unique identifier for non-transaction write commands.
 * It is used to lock the key for a specific command.
 *
 * @author Pedro Ruivo
 * @since 8.0
 */
@ProtoTypeId(ProtoStreamTypeIds.COMMAND_INVOCATION_ID)
public final class CommandInvocationId {
   public static final CommandInvocationId DUMMY_INVOCATION_ID = new CommandInvocationId(null, 0);

   private static final AtomicLong nextId = new AtomicLong(0);

   private final Address address;
   private final long id;

   @ProtoFactory
   CommandInvocationId(Address address, long id) {
      this.address = address;
      this.id = id;
   }

   @ProtoField(1)
   public long getId() {
      return id;
   }

   @ProtoField(2)
   public Address getAddress() {
      return address;
   }

   public static CommandInvocationId generateId(Address address) {
      return new CommandInvocationId(address, nextId.getAndIncrement());
   }

   public static CommandInvocationId generateIdFrom(CommandInvocationId commandInvocationId) {
      return new CommandInvocationId(commandInvocationId.address, nextId.getAndIncrement());
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) {
         return true;
      }
      if (o == null || getClass() != o.getClass()) {
         return false;
      }

      CommandInvocationId that = (CommandInvocationId) o;

      return id == that.id && Objects.equals(address, that.address);

   }

   @Override
   public int hashCode() {
      int result = address != null ? address.hashCode() : 0;
      result = 31 * result + (int) (id ^ (id >>> 32));
      return result;
   }

   @Override
   public String toString() {
      return "CommandInvocation:" + Objects.toString(address, "local") + ":" + id;
   }

   public static String show(CommandInvocationId id) {
      return id == DUMMY_INVOCATION_ID ? "" : id.toString();
   }
}

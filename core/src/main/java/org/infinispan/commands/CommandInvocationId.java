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
   public static final CommandInvocationId DUMMY_INVOCATION_ID = new CommandInvocationId(RequestUUID.NO_REQUEST);

   private static final AtomicLong nextId = new AtomicLong(0);

   private final RequestUUID requestUUID;

   CommandInvocationId(RequestUUID requestUUID) {
      this.requestUUID = Objects.requireNonNull(requestUUID);
   }

   @ProtoFactory
   static CommandInvocationId protoFactory(RequestUUID requestUUID) {
      // A transaction cache uses DUMMY_INVOCATION_ID
      // We check it here to avoid creating multiple instances of it.
      return Objects.equals(requestUUID, RequestUUID.NO_REQUEST) ?
            DUMMY_INVOCATION_ID :
            new CommandInvocationId(requestUUID);
   }

   public long getId() {
      return requestUUID.getRequestId();
   }

   public Address getAddress() {
      return requestUUID.toAddress();
   }

   @ProtoField(1)
   public RequestUUID getRequestUUID() {
      return requestUUID;
   }

   public static CommandInvocationId generateId(Address address) {
      return new CommandInvocationId(RequestUUID.of(address, nextId.getAndIncrement()));
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

      return requestUUID.equals(that.requestUUID);

   }

   @Override
   public int hashCode() {
      return requestUUID.hashCode();
   }

   @Override
   public String toString() {
      return "CommandInvocation:" + requestUUID.toIdString();
   }

   public static String show(CommandInvocationId id) {
      return id == DUMMY_INVOCATION_ID ? "" : id.toString();
   }
}

package org.infinispan.commands;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

import org.infinispan.remoting.transport.Address;

/**
 * Represents an unique identified for non-transaction write commands.
 *
 * It is used to lock the key for a specific command.
 *
 * This class is final to prevent issues as it is usually not marshalled
 * as polymorphic object but directly using {@link #writeTo(ObjectOutput, CommandInvocationId)}
 * and {@link #readFrom(ObjectInput)}.
 *
 * @author Pedro Ruivo
 * @since 8.0
 */
public final class CommandInvocationId {

   private static final AtomicLong nextId = new AtomicLong(0);

   private final Address address;
   private final long id;

   private CommandInvocationId(Address address, long id) {
      this.address = address;
      this.id = id;
   }

   public static CommandInvocationId generateId(Address address) {
      return new CommandInvocationId(address, nextId.getAndIncrement());
   }

   public static CommandInvocationId generateIdFrom(CommandInvocationId commandInvocationId) {
      return new CommandInvocationId(commandInvocationId.address, nextId.getAndIncrement());
   }


   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      CommandInvocationId that = (CommandInvocationId) o;

      return id == that.id && !(address != null ? !address.equals(that.address) : that.address != null);

   }

   public Address getAddress() {
      return address;
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

   public static void writeTo(ObjectOutput output, CommandInvocationId commandInvocationId) throws IOException {
      output.writeObject(commandInvocationId.address);
      output.writeLong(commandInvocationId.id);
   }

   public static CommandInvocationId readFrom(ObjectInput input) throws ClassNotFoundException, IOException {
      Address address = (Address) input.readObject();
      long id = input.readLong();
      return new CommandInvocationId(address, id);
   }

}

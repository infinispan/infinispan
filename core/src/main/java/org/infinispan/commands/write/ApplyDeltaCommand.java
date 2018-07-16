package org.infinispan.commands.write;

import java.io.IOException;
import java.io.ObjectInput;
import java.util.Collection;

import org.infinispan.atomic.Delta;
import org.infinispan.commands.CommandInvocationId;
import org.infinispan.commands.Visitor;
import org.infinispan.context.InvocationContext;
import org.infinispan.marshall.core.MarshalledEntryFactory;
import org.infinispan.marshall.core.UserAwareObjectOutput;


/**
 * This class can no longer be used. Any attempts to use it internally will lead to a {@link UnsupportedOperationException}
 * being thrown.
 * @author Vladimir Blagojevic
 * @since 5.1
 * @deprecated since 9.1
 */
@Deprecated
public class ApplyDeltaCommand extends AbstractDataWriteCommand {

   public static final int COMMAND_ID = 25;

   public ApplyDeltaCommand() {
   }

   public ApplyDeltaCommand(Object deltaAwareValueKey, Delta delta, Collection<Object> keys, CommandInvocationId commandInvocationId) {
      throw new UnsupportedOperationException();
   }

   public Delta getDelta(){
      throw new UnsupportedOperationException();
   }

   /**
    * Performs an application of delta on a specified entry
    *
    * @param ctx invocation context
    * @return null
    */
   @Override
   public Object perform(InvocationContext ctx) throws Throwable {
      throw new UnsupportedOperationException();
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public String toString() {
      return "ApplyDeltaCommand[" +
            "key=" + key +
            ", commandInvocationId=" + CommandInvocationId.show(commandInvocationId) +
            + ']';
   }

   @Override
   public void writeTo(UserAwareObjectOutput output, MarshalledEntryFactory entryFactory) throws IOException {
      throw new UnsupportedOperationException();
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      throw new UnsupportedOperationException();
   }

   @Override
   public Object acceptVisitor(InvocationContext ctx, Visitor visitor) throws Throwable {
      throw new UnsupportedOperationException();
   }

   public Object[] getKeys() {
      throw new UnsupportedOperationException();
   }

   public Object[] getCompositeKeys() {
      throw new UnsupportedOperationException();
   }

   @Override
   public LoadType loadType() {
      return LoadType.OWNER;
   }

   @Override
   public boolean isSuccessful() {
      return false;
   }

   @Override
   public boolean isConditional() {
      return false;
   }

   @Override
   public ValueMatcher getValueMatcher() {
      throw new UnsupportedOperationException();
   }

   @Override
   public void setValueMatcher(ValueMatcher valueMatcher) {
      // Do nothing
   }

   @Override
   public void fail() {
      throw new UnsupportedOperationException();
   }
}

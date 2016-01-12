package org.infinispan.commands.read;

import org.infinispan.commands.Visitor;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import static org.infinispan.commons.util.Util.toStr;

/**
 * Implements functionality defined by {@link org.infinispan.Cache#get(Object)} and
 * {@link org.infinispan.Cache#containsKey(Object)} operations
 *
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 * @since 4.0
 */
public class GetKeyValueCommand extends AbstractDataCommand implements RemoteFetchingCommand {

   public static final byte COMMAND_ID = 4;
   private static final Log log = LogFactory.getLog(GetKeyValueCommand.class);
   private static final boolean trace = log.isTraceEnabled();

   private InternalCacheEntry remotelyFetchedValue;

   public GetKeyValueCommand(Object key, long flagsBitSet) {
      super(key, flagsBitSet);
   }

   public GetKeyValueCommand() {
   }

   @Override
   public Object acceptVisitor(InvocationContext ctx, Visitor visitor) throws Throwable {
      return visitor.visitGetKeyValueCommand(ctx, this);
   }

   @Override
   public boolean readsExistingValues() {
      return true;
   }

   @Override
   public Object perform(InvocationContext ctx) throws Throwable {
      CacheEntry entry = ctx.lookupEntry(key);
      if (entry == null || entry.isNull()) {
         if (trace) {
            log.trace("Entry not found");
         }
         return null;
      }
      if (entry.isRemoved()) {
         if (trace) {
            log.tracef("Entry has been deleted and is of type %s", entry.getClass().getSimpleName());
         }
         return null;
      }

      return entry.getValue();
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public void writeTo(ObjectOutput output) throws IOException {
      output.writeObject(key);
      output.writeLong(Flag.copyWithoutRemotableFlags(getFlagsBitSet()));
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      key = input.readObject();
      setFlagsBitSet(input.readLong());
   }

   /**
    * @see #getRemotelyFetchedValue()
    */
   public void setRemotelyFetchedValue(InternalCacheEntry remotelyFetchedValue) {
      this.remotelyFetchedValue = remotelyFetchedValue;
   }

   /**
    * If the cache needs to go remotely in order to obtain the value associated to this key, then the remote value
    * is stored in this field.
    * TODO: this method should be able to removed with the refactoring from ISPN-2177
    */
   public InternalCacheEntry getRemotelyFetchedValue() {
      return remotelyFetchedValue;
   }

   public String toString() {
      return new StringBuilder()
            .append("GetKeyValueCommand {key=")
            .append(toStr(key))
            .append(", flags=").append(printFlags())
            .append("}")
            .toString();
   }

}

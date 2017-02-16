package org.infinispan.commands.read;

import static org.infinispan.commons.util.Util.toStr;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import org.infinispan.commands.Visitor;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Implements functionality defined by {@link org.infinispan.Cache#get(Object)} and
 * {@link org.infinispan.Cache#containsKey(Object)} operations
 *
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 * @since 4.0
 */
public class GetKeyValueCommand extends AbstractDataCommand {

   public static final byte COMMAND_ID = 4;
   private static final Log log = LogFactory.getLog(GetKeyValueCommand.class);
   private static final boolean trace = log.isTraceEnabled();

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
   public LoadType loadType() {
      return LoadType.OWNER;
   }

   @Override
   public Object perform(InvocationContext ctx) throws Throwable {
      CacheEntry entry = ctx.lookupEntry(key);
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
      output.writeLong(FlagBitSets.copyWithoutRemotableFlags(getFlagsBitSet()));
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      key = input.readObject();
      setFlagsBitSet(input.readLong());
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

package org.infinispan.commands.read;

import static org.infinispan.commons.util.Util.toStr;

import java.io.IOException;
import java.io.ObjectInput;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

import org.infinispan.commands.AbstractTopologyAffectedCommand;
import org.infinispan.commands.Visitor;
import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.impl.InternalEntryFactory;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.marshall.MarshalledEntryUtil;
import org.infinispan.marshall.core.MarshalledEntryFactory;
import org.infinispan.commons.marshall.UserObjectOutput;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Retrieves multiple entries at once.
 *
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
// TODO: revise the command hierarchy, e.g. this should not implement MetadataAwareCommand
public class GetAllCommand extends AbstractTopologyAffectedCommand {
   public static final byte COMMAND_ID = 44;
   private static final Log log = LogFactory.getLog(GetAllCommand.class);
   private static final boolean trace = log.isTraceEnabled();

   private Collection<?> keys;
   private boolean returnEntries;

   private /* transient */ InternalEntryFactory entryFactory;

   public GetAllCommand(Collection<?> keys, long flagsBitSet,
                        boolean returnEntries, InternalEntryFactory entryFactory) {
      this.keys = keys;
      this.returnEntries = returnEntries;
      this.entryFactory = entryFactory;
      setFlagsBitSet(flagsBitSet);
   }

   GetAllCommand() {
   }

   @Override
   public Object acceptVisitor(InvocationContext ctx, Visitor visitor) throws Throwable {
      return visitor.visitGetAllCommand(ctx, this);
   }

   @Override
   public LoadType loadType() {
      return LoadType.PRIMARY;
   }

   @Override
   public Object perform(InvocationContext ctx) throws Throwable {
      Map<Object, Object> map = createMap();
      for (Object key : keys) {
         CacheEntry entry = ctx.lookupEntry(key);
         if (entry == null) {
            throw new IllegalStateException("Entry for key " + toStr(key) + " not found");
         }
         if (entry.isNull()) {
            if (trace) {
               log.tracef("Entry for key %s is null in current context", toStr(key));
            }
            map.put(key, null);
            continue;
         }
         if (entry.isRemoved()) {
            if (trace) {
               log.tracef("Entry for key %s has been deleted and is of type %s", toStr(key), entry.getClass().getSimpleName());
            }
            map.put(key, null);
            continue;
         }

         // Get cache entry instead of value
         if (returnEntries) {
            CacheEntry copy;
            if (ctx.isOriginLocal()) {
               copy = entryFactory.copy(entry);
            } else {
               copy = entry;
            }
            if (trace) {
               log.tracef("Found entry %s -> %s", toStr(key), entry);
               log.tracef("Returning copied entry %s", copy);
            }
            map.put(key, copy);
         } else {
            Object value = entry.getValue();
            if (trace) {
               log.tracef("Found %s -> %s", toStr(key), toStr(value));
            }
            map.put(key, value);
         }
      }
      return map;
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public void writeTo(UserObjectOutput output, MarshalledEntryFactory entryFactory) throws IOException {
      output.writeUserCollection(keys, UserObjectOutput::writeKey);
      output.writeLong(FlagBitSets.copyWithoutRemotableFlags(getFlagsBitSet()));
      output.writeBoolean(returnEntries);
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      keys = MarshallUtil.unmarshallCollection(input, ArrayList::new, MarshalledEntryUtil::readKey);
      setFlagsBitSet(input.readLong());
      returnEntries = input.readBoolean();
   }

   @Override
   public boolean isReturnValueExpected() {
      return true;
   }

   @Override
   public boolean canBlock() {
      return false;
   }

   public boolean isReturnEntries() {
      return returnEntries;
   }

   public <V> Map<Object, V> createMap() {
      return new LinkedHashMap<>();
   }

   public Collection<?> getKeys() {
      return keys;
   }

   public void setKeys(Collection<?> keys) {
      this.keys = keys;
   }

   @Override
   public String toString() {
      final StringBuilder sb = new StringBuilder("GetAllCommand{");
      sb.append("keys=").append(toStr(keys));
      sb.append(", returnEntries=").append(returnEntries);
      sb.append(", flags=").append(printFlags());
      sb.append('}');
      return sb.toString();
   }
}

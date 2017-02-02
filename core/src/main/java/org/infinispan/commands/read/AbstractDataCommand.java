package org.infinispan.commands.read;

import static org.infinispan.commons.util.EnumUtil.prettyPrintBitSet;
import static org.infinispan.commons.util.Util.toStr;

import java.util.Objects;

import org.infinispan.commands.DataCommand;
import org.infinispan.context.Flag;

/**
 * @author Mircea.Markus@jboss.com
 * @author Sanne Grinovero <sanne@hibernate.org> (C) 2011 Red Hat Inc.
 * @since 4.0
 */
public abstract class AbstractDataCommand implements DataCommand {
   protected Object key;
   private long flags;
   private int topologyId = -1;

   protected AbstractDataCommand(Object key, long flagsBitSet) {
      this.key = key;
      this.flags = flagsBitSet;
   }

   protected AbstractDataCommand() {
   }

   @Override
   public int getTopologyId() {
      return topologyId;
   }

   @Override
   public void setTopologyId(int topologyId) {
      this.topologyId = topologyId;
   }

   @Override
   public long getFlagsBitSet() {
      return flags;
   }

   @Override
   public void setFlagsBitSet(long bitSet) {
      this.flags = bitSet;
   }

   @Override
   public Object getKey() {
      return key;
   }

   public void setKey(Object key) {
      this.key = key;
   }

   @Override
   public boolean equals(Object obj) {
      if (this == obj) {
         return true;
      }
      if (obj == null) {
         return false;
      }
      if (getClass() != obj.getClass()) {
         return false;
      }
      AbstractDataCommand other = (AbstractDataCommand) obj;
      return flags == other.flags && Objects.equals(key, other.key);
   }

   @Override
   public int hashCode() {
      return (key != null ? key.hashCode() : 0);
   }

   @Override
   public String toString() {
      return getClass().getSimpleName() +
            " {key=" + toStr(key) +
            ", flags=" + printFlags() +
            "}";
   }

   @Override
   public boolean isReturnValueExpected() {
      return true;
   }

   @Override
   public boolean canBlock() {
      return false;
   }

   protected final String printFlags() {
      return prettyPrintBitSet(flags, Flag.class);
   }
}

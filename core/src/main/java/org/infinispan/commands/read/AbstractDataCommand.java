package org.infinispan.commands.read;

import static org.infinispan.commons.util.EnumUtil.prettyPrintBitSet;
import static org.infinispan.commons.util.Util.toStr;

import java.util.Objects;

import org.infinispan.commands.DataCommand;
import org.infinispan.commands.SegmentSpecificCommand;
import org.infinispan.context.Flag;

/**
 * @author Mircea.Markus@jboss.com
 * @author Sanne Grinovero &lt;sanne@hibernate.org&gt; (C) 2011 Red Hat Inc.
 * @since 4.0
 */
public abstract class AbstractDataCommand implements DataCommand, SegmentSpecificCommand {
   protected Object key;
   private long flags;
   // These 2 ints have to stay next to each other to ensure they are aligned together
   private int topologyId = -1;
   protected int segment;

   protected AbstractDataCommand(Object key, int segment, long flagsBitSet) {
      this.key = key;
      if (segment < 0) {
         throw new IllegalArgumentException("Segment must be 0 or greater");
      }
      this.segment = segment;
      this.flags = flagsBitSet;
   }

   protected AbstractDataCommand() {
      this.segment = -1;
   }

   @Override
   public int getSegment() {
      return segment;
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

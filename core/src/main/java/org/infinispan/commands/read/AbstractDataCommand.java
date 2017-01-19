package org.infinispan.commands.read;

import static org.infinispan.commons.util.Util.toStr;

import java.util.Objects;

import org.infinispan.commands.AbstractTopologyAffectedCommand;
import org.infinispan.commands.DataCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.lifecycle.ComponentStatus;

/**
 * @author Mircea.Markus@jboss.com
 * @author Sanne Grinovero <sanne@hibernate.org> (C) 2011 Red Hat Inc.
 * @since 4.0
 */
public abstract class AbstractDataCommand extends AbstractTopologyAffectedCommand implements DataCommand {
   protected Object key;
   private long flags;

   protected AbstractDataCommand(Object key, long flagsBitSet) {
      this.key = key;
      this.flags = flagsBitSet;
   }

   protected AbstractDataCommand() {
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
   public boolean shouldInvoke(InvocationContext ctx) {
      return true;
   }

   @Override
   public boolean ignoreCommandOnStatus(ComponentStatus status) {
      return false;
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
}

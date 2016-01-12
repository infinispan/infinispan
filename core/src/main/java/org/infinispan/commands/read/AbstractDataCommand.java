package org.infinispan.commands.read;

import org.infinispan.commands.AbstractFlagAffectedCommand;
import org.infinispan.commands.DataCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.lifecycle.ComponentStatus;

import static org.infinispan.commons.util.Util.toStr;

/**
 * @author Mircea.Markus@jboss.com
 * @author Sanne Grinovero <sanne@hibernate.org> (C) 2011 Red Hat Inc.
 * @since 4.0
 */
public abstract class AbstractDataCommand extends AbstractFlagAffectedCommand implements DataCommand {
   protected Object key;

   @Override
   public Object getKey() {
      return key;
   }

   public void setKey(Object key) {
      this.key = key;
   }

   protected AbstractDataCommand(Object key, long flagsBitSet) {
      this.key = key;
      setFlagsBitSet(flagsBitSet);
   }

   protected AbstractDataCommand() {
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
      if (this == obj)
         return true;
      if (obj == null)
         return false;
      if (getClass() != obj.getClass())
         return false;
      AbstractDataCommand other = (AbstractDataCommand) obj;
      if (key == null) {
         if (other.key != null)
            return false;
      } else if (!key.equals(other.key))
         return false;
      if (!hasSameFlags(other))
         return false;
      return true;
   }
   
   @Override
   public int hashCode() {
      return (key != null ? key.hashCode() : 0);
   }
   
   @Override
   public String toString() {
      return new StringBuilder(getClass().getSimpleName())
         .append(" {key=")
         .append(toStr(key))
         .append(", flags=").append(printFlags())
         .append("}")
         .toString();
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

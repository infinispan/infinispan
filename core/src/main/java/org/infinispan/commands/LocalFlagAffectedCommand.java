package org.infinispan.commands;

import org.infinispan.commons.util.EnumUtil;
import org.infinispan.context.Flag;

import java.util.Arrays;
import java.util.Set;

/**
 * Commands affected by Flags will be checked locally to control certain behaviors such whether or not to invoke
 * certain commands remotely, check cache store etc.
 *
 * @author William Burns
 * @since 6.0
 */
public interface LocalFlagAffectedCommand {
   /**
    * @return The command flags - only valid to invoke after {@link #setFlags(java.util.Set)}. The set should
    * not be modified directly, only via the {@link #setFlags(Set)}, {@link #addFlag(Flag)} and {@link
    * #addFlags(Set)} methods.
    */
   default Set<Flag> getFlags() {
      return EnumUtil.enumSetOf(getFlagsBitSet(), Flag.class);
   }

   long getFlagsBitSet();

   /**
    * Set the flags, replacing any existing flags.
    *
    * @param flags The new flags.
    */
   default void setFlags(Set<Flag> flags) {
      setFlagsBitSet(EnumUtil.bitSetOf(flags));
   }

   void setFlagsBitSet(long bitSet);

   /**
    * Add some flags to the command.
    *
    * @deprecated Use either {@link #addFlag(Flag)} or {@link #addFlags(Set)} instead.
    *
    * @param newFlags The flags to add.
    */
   @Deprecated
   default void setFlags(Flag... newFlags) {
      setFlagsBitSet(EnumUtil.setEnums(getFlagsBitSet(), Arrays.asList(newFlags)));
   }

   /**
    * Add a single flag to the command.
    *
    * @param flag The flag to add.
    */
   default void addFlag(Flag flag) {
      setFlagsBitSet(EnumUtil.setEnum(getFlagsBitSet(), flag));
   }

   /**
    * Add a set of flags to the command.
    *
    * @param flags The flags to add.
    */
   default void addFlags(Set<Flag> flags) {
      setFlagsBitSet(EnumUtil.setEnums(getFlagsBitSet(), flags));
   }

   /**
    * Check whether a particular flag is present in the command
    *
    * @param flag to lookup in the command
    * @return true if the flag is present
    */
   default boolean hasFlag(Flag flag) {
      return EnumUtil.hasEnum(getFlagsBitSet(), flag);
   }
}

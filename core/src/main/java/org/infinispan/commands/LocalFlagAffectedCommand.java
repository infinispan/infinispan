package org.infinispan.commands;

import org.infinispan.context.Flag;

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
   Set<Flag> getFlags();

   /**
    * Set the flags, replacing any existing flags.
    *
    * @param flags The new flags.
    */
   void setFlags(Set<Flag> flags);

   /**
    * Add some flags to the command.
    *
    * @deprecated Use either {@link #addFlag(Flag)} or {@link #addFlags(Set)} instead.
    *
    * @param newFlags The flags to add.
    */
   @Deprecated
   default void setFlags(Flag... newFlags) {
      setFlags(Flag.addFlags(getFlags(), newFlags));
   }

   /**
    * Add a single flag to the command.
    *
    * @param flag The flag to add.
    */
   default void addFlag(Flag flag) {
      setFlags(Flag.addFlag(getFlags(), flag));
   }

   /**
    * Add a set of flags to the command.
    *
    * @param flags The flags to add.
    */
   default void addFlags(Set<Flag> flags) {
      setFlags(Flag.addFlags(getFlags(), flags));
   }

   /**
    * Check whether a particular flag is present in the command
    *
    * @param flag to lookup in the command
    * @return true if the flag is present
    */
   default boolean hasFlag(Flag flag) {
      Set<Flag> flags = getFlags();
      return flags != null && flags.contains(flag);
   }
}

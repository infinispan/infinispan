package org.infinispan.commands;

import java.util.Set;

import org.infinispan.commons.util.EnumUtil;
import org.infinispan.context.Flag;
import org.infinispan.context.impl.FlagBitSets;

/**
 * Flags modify behavior of command such as whether or not to invoke certain commands remotely, check cache store etc.
 *
 * @author William Burns
 * @author Sanne Grinovero
 * @since 5.0
 */
public interface FlagAffectedCommand extends VisitableCommand {
   /**
    * @return The command flags - only valid to invoke after {@link #setFlags(java.util.Set)}. The set should
    * not be modified directly, only via the {@link #setFlags(Set)}, {@link #addFlag(Flag)} and {@link
    * #addFlags(Set)} methods.
    */
   default Set<Flag> getFlags() {
      return EnumUtil.enumSetOf(getFlagsBitSet(), Flag.class);
   }

   /**
    * @return The command flags. Flags can be modified with {@link #setFlagsBitSet(long)}, {@link #addFlags(long)}
    * and {@link #addFlags(Set)} methods.
    */
   long getFlagsBitSet();

   /**
    * Set the flags, replacing any existing flags.
    *
    * @param flags The new flags.
    * @deprecated Since 9.0, please use {@link #setFlagsBitSet(long)} instead.
    */
   default void setFlags(Set<Flag> flags) {
      setFlagsBitSet(EnumUtil.bitSetOf(flags));
   }

   /**
    * Set the flags, replacing any existing flags.
    */
   void setFlagsBitSet(long bitSet);

   /**
    * Add a single flag to the command.
    *
    * @param flag The flag to add.
    *
    * @deprecated Since 9.0, please use {@link #addFlags(long)} with a {@link FlagBitSets} constant instead.
    */
   @Deprecated
   default void addFlag(Flag flag) {
      setFlagsBitSet(EnumUtil.setEnum(getFlagsBitSet(), flag));
   }

   /**
    * Add a set of flags to the command.
    *
    * @param flags The flags to add.
    *
    * @deprecated Since 9.0, please use {@link #addFlags(long)} with a {@link FlagBitSets} constant instead.
    */
   @Deprecated
   default void addFlags(Set<Flag> flags) {
      setFlagsBitSet(EnumUtil.setEnums(getFlagsBitSet(), flags));
   }

   /**
    * Add a set of flags to the command.
    *
    * @param flagsBitSet The flags to add, usually a {@link FlagBitSets} constant (or combination thereof).
    */
   default void addFlags(long flagsBitSet) {
      setFlagsBitSet(EnumUtil.mergeBitSets(getFlagsBitSet(), flagsBitSet));
   }

   /**
    * Check whether a particular flag is present in the command.
    *
    * @param flag to lookup in the command
    * @return true if the flag is present
    *
    * @deprecated Since 9.0, please use {@link #hasAnyFlag(long)} with a {@link FlagBitSets} constant instead.
    */
   @Deprecated
   default boolean hasFlag(Flag flag) {
      return EnumUtil.hasEnum(getFlagsBitSet(), flag);
   }

   /**
    * Check whether any of the flags in the {@code flagsBitSet} parameter is present in the command.
    *
    * Should be used with the constants in {@link FlagBitSets}.
    */
   default boolean hasAnyFlag(long flagsBitSet) {
      return EnumUtil.containsAny(getFlagsBitSet(), flagsBitSet);
   }

   /**
    * Check whether all of the flags in the {@code flagsBitSet} parameter are present in the command.
    *
    * Should be used with the constants in {@link FlagBitSets}.
    */
   default boolean hasAllFlags(long flagBitSet) {
      return EnumUtil.containsAll(getFlagsBitSet(), flagBitSet);
   }
}

package org.infinispan.container.entries;

/**
 * An entry that may have state, such as created, changed, valid, etc.
 *
 * @author Manik Surtani
 * @since 5.1
 * @deprecated since 9.0
 */
@Deprecated
public interface StateChangingEntry {

   @Deprecated
   default byte getStateFlags() {
      return 0;
   }

   @Deprecated
   default void copyStateFlagsFrom(StateChangingEntry other) {}

}

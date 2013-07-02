package org.infinispan.container.entries;

/**
 * An entry that may have state, such as created, changed, valid, etc.
 *
 * @author Manik Surtani
 * @since 5.1
 */
public interface StateChangingEntry {

   byte getStateFlags();

   void copyStateFlagsFrom(StateChangingEntry other);

}

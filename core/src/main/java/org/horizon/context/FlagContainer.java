package org.horizon.context;

import org.horizon.invocation.Flag;

import java.util.Collection;
import java.util.Set;

/**
 * Interface that defines access to and manipulation of {@link org.horizon.invocation.Flag}.
 *
 * @author Manik Surtani
 * @since 4.0
 */
public interface FlagContainer {

   boolean hasFlag(Flag o);

   Set<Flag> getFlags();

   void setFlags(Flag... flags);

   void setFlags(Collection<Flag> flags);

   void resetFlags();

   boolean isFlagsUninitialized();
}

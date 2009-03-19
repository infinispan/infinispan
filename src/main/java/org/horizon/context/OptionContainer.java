package org.horizon.context;

import org.horizon.invocation.Options;

import java.util.Collection;
import java.util.Set;

/**
 * Interface that defines access to and manipulation of {@link Options}.
 *
 * @author Manik Surtani
 * @since 4.0
 */
public interface OptionContainer {

   boolean hasOption(Options o);

   Set<Options> getOptions();

   void setOptions(Options... options);

   void setOptions(Collection<Options> options);

   void resetOptions();

   boolean isOptionsUninit();
}

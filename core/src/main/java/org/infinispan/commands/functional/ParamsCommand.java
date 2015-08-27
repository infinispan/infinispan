package org.infinispan.commands.functional;

import org.infinispan.functional.impl.Params;

/**
 * A command that carries parameters.
 */
public interface ParamsCommand {

   Params getParams();

}

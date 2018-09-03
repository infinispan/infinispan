package org.infinispan.globalstate.impl;

import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.globalstate.GlobalConfigurationManager;

@Scope(Scopes.GLOBAL)
public class GlobalConfigurationManagerPostStart {
   @Inject private GlobalConfigurationManager gcm;
}

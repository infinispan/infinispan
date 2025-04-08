package org.infinispan.hibernate.cache.commons.util;

import org.infinispan.commands.module.ModuleCommandExtensions;
import org.infinispan.commands.module.ModuleCommandFactory;
import org.kohsuke.MetaInfServices;

/**
 * Command extensions for second-level cache use case
 *
 * @author Galder Zamarreño
 * @since 4.0
 */
@MetaInfServices(ModuleCommandExtensions.class)
public class CacheCommandExtensions implements ModuleCommandExtensions {
	final CacheCommandFactory cacheCommandFactory = new CacheCommandFactory();

	@Override
	public ModuleCommandFactory getModuleCommandFactory() {
		return cacheCommandFactory;
	}
}

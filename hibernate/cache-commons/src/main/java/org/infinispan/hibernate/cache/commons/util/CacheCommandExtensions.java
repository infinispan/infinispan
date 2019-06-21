/*
 * Hibernate, Relational Persistence for Idiomatic Java
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later.
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
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

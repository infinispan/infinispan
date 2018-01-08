/*
 * Hibernate, Relational Persistence for Idiomatic Java
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later.
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.infinispan.hibernate.cache.commons.util;

import org.infinispan.factories.components.ModuleMetadataFileFinder;
import org.kohsuke.MetaInfServices;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
@MetaInfServices(ModuleMetadataFileFinder.class)
public class SecondLevelCacheMetadataModuleFinder implements ModuleMetadataFileFinder {
	@Override
	public String getMetadataFilename() {
		return "infinispan-hibernate-cache-commons-component-metadata.dat";
	}
}

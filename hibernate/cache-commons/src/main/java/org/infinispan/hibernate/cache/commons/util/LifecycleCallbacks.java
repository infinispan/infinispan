/*
 * Hibernate, Relational Persistence for Idiomatic Java
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later.
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.infinispan.hibernate.cache.commons.util;

import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.factories.annotations.InfinispanModule;
import org.infinispan.lifecycle.ModuleLifecycle;
import org.infinispan.marshall.protostream.impl.SerializationContextRegistry;

@InfinispanModule(name = "hibernate-cache-commons", requiredModules = "core")
public class LifecycleCallbacks implements ModuleLifecycle {

	@Override
	public void cacheManagerStarting(GlobalComponentRegistry gcr, GlobalConfiguration globalCfg) {
		SerializationContextRegistry ctxRegistry = gcr.getComponent(SerializationContextRegistry.class);
		ctxRegistry.addContextInitializer(SerializationContextRegistry.MarshallerType.GLOBAL, new org.infinispan.hibernate.cache.commons.GlobalContextInitializerImpl());
	}
}

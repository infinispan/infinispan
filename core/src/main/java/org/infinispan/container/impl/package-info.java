/**
 * Data containers which store cache entries.  This package contains different implementations of
 * containers based on their performance and ordering characteristics, as well as the entries
 * that live in the containers.
 * <p />
 * This package also contains the factory for creating entries, and is typically used by the {@link LockingInterceptor}
 * to wrap an entry and put it in a thread's {@link InvocationContext}
 */
package org.infinispan.container.impl;

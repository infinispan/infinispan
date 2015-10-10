package org.infinispan.interceptors;

import org.infinispan.commands.CommandsFactory;
import org.infinispan.container.DataContainer;
import org.infinispan.container.EntryFactory;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.base.BaseRpcInterceptor;
import org.infinispan.statetransfer.StateTransferManager;
import org.infinispan.util.concurrent.locks.LockManager;

/**
 * Base class for replication and distribution interceptors.
 *
 * @author anistor@redhat.com
 * @deprecated Since 8.1, use {@link org.infinispan.interceptors.sequential.ClusteringInterceptor} instead.
 */
@Deprecated
public abstract class ClusteringInterceptor extends BaseRpcInterceptor {
}

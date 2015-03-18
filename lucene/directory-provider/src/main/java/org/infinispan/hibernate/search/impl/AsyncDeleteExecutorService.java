package org.infinispan.hibernate.search.impl;

import org.hibernate.search.engine.service.spi.Service;

import java.util.concurrent.Executor;

/**
 * Defines the service contract for the Executor which we'll use in combination with the Infinispan Lucene Directory, as
 * this provides an option to execute delete operations in background. It is important to run delete operations in
 * background as while these are simple from a computational point of view, they will introduce a significant delay on
 * write operations when Infinispan is running in clustered mode. This is implemented as a Service so that integrations
 * can inject a different managed threadpool, and we can share the same executor among multiple IndexManagers.
 *
 * @author Sanne Grinovero <sanne@hibernate.org> (C) 2014 Red Hat Inc.
 */
public interface AsyncDeleteExecutorService extends Service {

   Executor getExecutor();

   void closeAndFlush();

}

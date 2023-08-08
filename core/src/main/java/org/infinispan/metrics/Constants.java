package org.infinispan.metrics;

/**
 * Various public constant names, used by Infinispan's metrics support.
 *
 * @author anistor@redhat.com
 * @since 10.1
 */
public interface Constants {

   String NODE_TAG_NAME = "node";

   String CACHE_MANAGER_TAG_NAME = "cache_manager";

   String CACHE_TAG_NAME = "cache";

   String JGROUPS_PREFIX = "jgroups_";

   String JGROUPS_CLUSTER_TAG_NAME = "cluster";

   String SITE_TAG_NAME = "site";
}

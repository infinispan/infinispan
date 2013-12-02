package org.infinispan.query.impl;

/**
 * MBean interface as required by JMX rules. It duplicates org.hibernate.search.jmx.StatisticsInfoMBean
 * just to be in the same package as org.infinispan.query.impl.InfinispanQueryStatisticsInfo.
 *
 * @author anistor@redhat.com
 * @since 6.1
 */
public interface InfinispanQueryStatisticsInfoMBean extends org.hibernate.search.jmx.StatisticsInfoMBean {
}

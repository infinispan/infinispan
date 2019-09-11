package org.infinispan.query.impl;

/**
 * MBean interface as required by JMX rules. It duplicates {@link org.hibernate.search.jmx.StatisticsInfoMBean} just to
 * be in the same package as {@link org.infinispan.query.impl.InfinispanQueryStatisticsInfo} so the MBean magic can
 * work.
 *
 * @author anistor@redhat.com
 * @since 6.1
 */
public interface InfinispanQueryStatisticsInfoMBean extends org.hibernate.search.jmx.StatisticsInfoMBean {
}

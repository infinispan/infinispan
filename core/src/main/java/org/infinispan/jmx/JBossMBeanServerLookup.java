package org.infinispan.jmx;

/**
 * MBeanServer lookup implementation to locate the JBoss MBeanServer.
 *
 * @author Galder Zamarreño
 * @since 4.2
 * @deprecated since 9.4, use {@link org.infinispan.commons.jmx.JBossMBeanServerLookup} instead
 */
@Deprecated
public class JBossMBeanServerLookup extends org.infinispan.commons.jmx.JBossMBeanServerLookup implements MBeanServerLookup {
}

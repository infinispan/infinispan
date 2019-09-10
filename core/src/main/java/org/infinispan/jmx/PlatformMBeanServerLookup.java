package org.infinispan.jmx;

/**
 * Default implementation for {@link MBeanServerLookup}, will return the platform MBean server.
 * <p/>
 * Note: to enable platform MBeanServer the following system property should be passed to the Sun JVM:
 * <b>-Dcom.sun.management.jmxremote</b>.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 * @deprecated Use {@link org.infinispan.commons.jmx.PlatformMBeanServerLookup} instead
 */
@Deprecated
public class PlatformMBeanServerLookup extends org.infinispan.commons.jmx.PlatformMBeanServerLookup implements MBeanServerLookup {
}

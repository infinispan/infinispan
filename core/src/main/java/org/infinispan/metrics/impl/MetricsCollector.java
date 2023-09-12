package org.infinispan.metrics.impl;

import java.net.InetAddress;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

import org.infinispan.commons.stat.MetricInfo;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.SurvivesRestarts;
import org.infinispan.factories.impl.ComponentRef;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.metrics.Constants;
import org.infinispan.remoting.transport.Transport;

/**
 * Keeps a reference to the Micrometer MeterRegistry. Optional component in component registry. Availability based on
 * available jars in classpath! See {@link MetricsComponentFactory}.
 *
 * @author anistor@redhat.com
 * @since 10.1
 */
@Scope(Scopes.GLOBAL)
@SurvivesRestarts
public class MetricsCollector implements Constants {

   @Inject MetricsRegistry metricsRegistry;
   @Inject GlobalConfiguration globalConfig;
   // still required to avoid (a long) cyclic dependency
   @Inject ComponentRef<Transport> transportRef;

   private String nodeName;

   MetricsCollector() {
   }

   @Start
   protected void start() {
      Transport transport = transportRef.running();
      nodeName = transport != null ? transport.getAddress().toString() : globalConfig.transport().nodeName();
      if (nodeName == null) {
         //TODO [anistor] Maybe we should just ensure a unique node name was set in all tests and also in real life usage, even for local cache managers
         nodeName = generateRandomName();
         //throw new CacheConfigurationException("Node name must always be specified in configuration if metrics are enabled.");
      }
   }

   /**
    * Generate a not so random name based on host name.
    */
   private static String generateRandomName() {
      String hostName = null;
      try {
         hostName = InetAddress.getLocalHost().getHostName();
      } catch (Throwable ignored) {
      }
      if (hostName == null) {
         try {
            hostName = InetAddress.getByName(null).getHostName();
         } catch (Throwable ignored) {
         }
      }
      if (hostName == null) {
         hostName = "localhost";
      } else {
         int dotPos = hostName.indexOf('.');
         if (dotPos > 0 && !Character.isDigit(hostName.charAt(0))) {
            hostName = hostName.substring(0, dotPos);
         }
      }
      int rand = 1 + ThreadLocalRandom.current().nextInt(Short.MAX_VALUE * 2);
      return hostName + '-' + rand;
   }

   public Set<Object> registerMetrics(Object instance, Collection<MetricInfo> metrics, String namePrefix, String cacheName) {
      Map<String, String> tags;
      if (cacheName != null) {
         tags = Map.of(CACHE_TAG_NAME, cacheName, NODE_TAG_NAME, nodeName);
      } else {
         tags = Map.of(NODE_TAG_NAME, nodeName);
      }
      return metricsRegistry.registerMetrics(instance, metrics, namePrefix, tags);
   }

   public void unregisterMetric(Object metricId) {
      metricsRegistry.unregisterMetric(metricId);
   }

}

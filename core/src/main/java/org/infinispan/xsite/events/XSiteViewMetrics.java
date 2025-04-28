package org.infinispan.xsite.events;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import org.infinispan.commons.stat.MetricInfo;
import org.infinispan.metrics.Constants;
import org.infinispan.metrics.impl.MetricUtils;
import org.infinispan.metrics.impl.MetricsRegistry;
import org.infinispan.remoting.transport.Transport;

class XSiteViewMetrics implements Constants {

   private static final int OFFLINE = 0;
   private static final int ONLINE = 1;
   private static final int UNKNOWN = 2;
   private static final Collection<MetricInfo> METRIC = List.of(MetricUtils.createGauge("SiteViewStatus", "Returns the site status in JGroups view (0 -> offline, 1 -> online, 2 -> n/a)", AtomicInteger::get, null));

   private final Map<String, SiteStatusMetric> siteMetrics;
   private final Supplier<MetricsRegistry> metricsRegistrySupplier;
   private final Supplier<Transport> transportSupplier;

   XSiteViewMetrics(Supplier<MetricsRegistry> metricsRegistrySupplier, Supplier<Transport> transportSupplier) {
      this.metricsRegistrySupplier = metricsRegistrySupplier;
      this.transportSupplier = transportSupplier;
      siteMetrics = new ConcurrentHashMap<>(2);
   }

   void stop() {
      var registry = metricsRegistrySupplier.get();
      siteMetrics.values().stream().map(SiteStatusMetric::metricId).forEach(registry::unregisterMetric);
      siteMetrics.clear();
   }

   void onNewCrossSiteView(Collection<String> joiners, Collection<String> leavers) {
      joiners.forEach(this::handleJoiner);
      leavers.forEach(this::handleLeaver);
   }

   void onSiteCoordinatorPromotion(Collection<String> siteView) {
      siteMetrics.forEach((siteName, metric) -> metric.status.set(siteView.contains(siteName) ? ONLINE : OFFLINE));
   }

   void markAllUnknown() {
      siteMetrics.forEach((siteName, metric) -> metric.status.set(UNKNOWN));
   }

   void onNewSiteFound(String siteName, Collection<String> siteView) {
      var metric = siteMetrics.computeIfAbsent(siteName, this::create);
      if (siteView != null) {
         metric.status.set(siteView.contains(siteName) ? ONLINE : OFFLINE);
      }
   }

   private void handleJoiner(String siteName) {
      siteMetrics.computeIfPresent(siteName, this::setOnline);
   }

   private void handleLeaver(String siteName) {
      siteMetrics.computeIfPresent(siteName, this::setOffline);
   }

   private SiteStatusMetric create(String siteName) {
      var registry = metricsRegistrySupplier.get();
      var status = new AtomicInteger(UNKNOWN);
      var metricId = registry.legacy()
            ? registry.registerMetrics(status, METRIC, VENDOR_PREFIX + JGROUPS_PREFIX, metricTags(siteName))
            : registry.registerMetrics(status, METRIC, JGROUPS_PREFIX, metricTags(siteName));
      return new SiteStatusMetric(metricId, status);
   }

   private SiteStatusMetric setOnline(String siteName, SiteStatusMetric metric) {
      metric.status.set(ONLINE);
      return metric;
   }

   private SiteStatusMetric setOffline(String siteName, SiteStatusMetric metric) {
      metric.status.set(OFFLINE);
      return metric;
   }

   private Map<String, String> metricTags(String siteName) {
      var transport = transportSupplier.get();
      return Map.of(
            NODE_TAG_NAME, String.valueOf(transport.getAddress()),
            SITE_TAG_NAME, siteName
      );
   }

   private record SiteStatusMetric(Object metricId, AtomicInteger status) {
   }
}

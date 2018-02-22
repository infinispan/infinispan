/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2012, Red Hat, Inc., and individual contributors
 * as indicated by the @author tags. See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */

package org.jboss.as.clustering.infinispan.subsystem;

import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.OP_ADDR;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.input.ReversedLinesFileReader;
import org.infinispan.health.CacheHealth;
import org.infinispan.health.Health;
import org.infinispan.server.infinispan.spi.service.CacheContainerServiceName;
import org.jboss.as.clustering.infinispan.DefaultCacheContainer;
import org.jboss.as.controller.AbstractRuntimeOnlyHandler;
import org.jboss.as.controller.AttributeDefinition;
import org.jboss.as.controller.OperationContext;
import org.jboss.as.controller.PathAddress;
import org.jboss.as.controller.StringListAttributeDefinition;
import org.jboss.as.controller.descriptions.ModelDescriptionConstants;
import org.jboss.as.controller.registry.ManagementResourceRegistration;
import org.jboss.as.controller.services.path.PathManager;
import org.jboss.as.server.ServerEnvironment;
import org.jboss.dmr.ModelNode;
import org.jboss.dmr.ModelType;
import org.jboss.msc.service.ServiceController;

/**
 * Attaches HealCheck API as DMR Metrics.
 *
 * @author Sebastian Łaskawiec
 * @since 9.0
 */
public class HealthMetricsHandler extends AbstractRuntimeOnlyHandler {

    public static final HealthMetricsHandler INSTANCE = new HealthMetricsHandler();

    private static final int CACHE_CONTAINER_INDEX = 1;
    private static final int NUMBER_OF_LINES = 10;

    private PathManager pathManager;

    private static Collection<ModelNode> toModelNodeCollection(Collection<String> collection) {
        if (collection == null || collection.isEmpty()) {
            return Collections.emptyList();
        }
        Collection<ModelNode> modelNodeCollection = new ArrayList<>(collection.size());
        collection.forEach(e -> modelNodeCollection.add(new ModelNode().set(e)));
        return modelNodeCollection;
    }

    @Override
    protected void executeRuntimeStep(OperationContext context, ModelNode operation) {
        /*
         * This is a kind of operation we are parsing:
         *    {
         *       "operation" => "read-attribute",
         *       "address" => [
         *           ("subsystem" => "datagrid-infinispan"),
         *           ("cache-container" => "clustered"),
         *           ("health" => "HEALTH")
         *       ],
         *       "name" => "number-of-nodes",
         *       "include-defaults" => true,
         *       "resolve-expressions" => false,
         *       "operation-headers" => {
         *           "caller-type" => "user",
         *           "access-mechanism" => "NATIVE"
         *       }
         *   }
         */
        final ModelNode result = new ModelNode();
        final PathAddress address = PathAddress.pathAddress(operation.require(OP_ADDR));
        final String cacheContainerName = address.getElement(CACHE_CONTAINER_INDEX).getValue();
        final String metricName = operation.require(ModelDescriptionConstants.NAME).asString();
        final ServiceController<?> controller = context.getServiceRegistry(false).getService(CacheContainerServiceName.CACHE_CONTAINER.getServiceName(cacheContainerName));

        if(controller != null) {
            DefaultCacheContainer cacheManager = (DefaultCacheContainer) controller.getValue();
            HealthMetrics metric = HealthMetrics.getMetric(metricName);

            if (metric == null) {
                context.getFailureDescription().set(String.format("Unknown metric %s", metricName));
            } else if (cacheManager == null) {
                context.getFailureDescription().set(String.format("Unavailable cache container %s", metricName));
            } else {
                Health health = cacheManager.getHealth();
                switch (metric) {
                    case CACHE_HEALTH:
                        List<CacheHealth> cacheHealths = health.getCacheHealth();
                        List<String> perCacheHealth = new LinkedList<>();
                        for (CacheHealth cacheHealth : cacheHealths) {
                            perCacheHealth.add(cacheHealth.getCacheName());
                            perCacheHealth.add(cacheHealth.getStatus().toString());
                        }
                        result.set(toModelNodeCollection(perCacheHealth));
                        break;
                    case FREE_MEMORY_KB:
                        result.set(health.getHostInfo().getFreeMemoryInKb());
                        break;
                    case TOTAL_MEMORY_KB:
                        result.set(health.getHostInfo().getTotalMemoryKb());
                        break;
                    case NUMBER_OF_NODES:
                        result.set(health.getClusterHealth().getNumberOfNodes());
                        break;
                    case CLUSTER_NAME:
                        result.set(health.getClusterHealth().getClusterName());
                        break;
                    case NUMBER_OF_CPUS:
                        result.set(health.getHostInfo().getNumberOfCpus());
                        break;
                    case CLUSTER_HEALTH:
                        result.set(health.getClusterHealth().getHealthStatus().toString());
                        break;
                    case LOG_TAIL:
                        File path = new File(pathManager.resolveRelativePathEntry("server.log", ServerEnvironment.SERVER_LOG_DIR));
                        try (ReversedLinesFileReader reader = new ReversedLinesFileReader(path, StandardCharsets.UTF_8)) {
                            List<String> results = new LinkedList<>();
                            for (int i = 0; i < NUMBER_OF_LINES; ++i) {
                                results.add(0, reader.readLine());
                            }
                            result.set(toModelNodeCollection(results));
                        } catch (FileNotFoundException e) {
                            result.set("File [" + path.getAbsolutePath() + "] does not exist");
                        } catch (IOException e) {
                            result.set("Unable to read file [" + path.getAbsolutePath() + "]");
                        }
                        break;
                    default:
                        context.getFailureDescription().set(String.format("Unknown metric %s", metric));
                        break;
                }

            }
        }
        context.getResult().set(result);
    }

    public void registerPathManager(PathManager pathManager) {
        this.pathManager = pathManager;
    }

    public void registerMetrics(ManagementResourceRegistration container) {
        for (HealthMetrics metric : HealthMetrics.values()) {
            container.registerMetric(metric.definition, this);
        }
    }

    public enum HealthMetrics {
        NUMBER_OF_CPUS(MetricKeys.NUMBER_OF_CPUS, ModelType.INT),
        TOTAL_MEMORY_KB(MetricKeys.TOTAL_MEMORY_KB, ModelType.LONG),
        FREE_MEMORY_KB(MetricKeys.FREE_MEMORY_KB, ModelType.LONG),
        CLUSTER_HEALTH(MetricKeys.CLUSTER_HEALTH, ModelType.STRING),
        CLUSTER_NAME(MetricKeys.CLUSTER_NAME, ModelType.STRING),
        NUMBER_OF_NODES(MetricKeys.NUMBER_OF_NODES, ModelType.INT),
        CACHE_HEALTH(MetricKeys.CACHE_HEALTH, ModelType.LIST),
        LOG_TAIL(MetricKeys.LOG_TAIL, ModelType.LIST);

        private static final Map<String, HealthMetrics> MAP = new HashMap<>();

        static {
            for (HealthMetrics metric : HealthMetrics.values()) {
                MAP.put(metric.toString(), metric);
            }
        }

        final AttributeDefinition definition;

        HealthMetrics(String attributeName, ModelType type) {
            this.definition = new StringListAttributeDefinition.Builder(attributeName)
                    .setRequired(false)
                    .setStorageRuntime()
                    .build();
        }

        public static HealthMetrics getMetric(final String stringForm) {
            return MAP.get(stringForm);
        }

        @Override
        public final String toString() {
            return definition.getName();
        }
    }
}

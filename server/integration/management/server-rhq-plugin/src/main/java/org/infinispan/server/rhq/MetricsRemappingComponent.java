/*
 * JBoss, Home of Professional Open Source
 * Copyright 2013 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA.
 */
package org.infinispan.server.rhq;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.rhq.core.domain.measurement.DataType;
import org.rhq.core.domain.measurement.MeasurementDataNumeric;
import org.rhq.core.domain.measurement.MeasurementDataTrait;
import org.rhq.core.domain.measurement.MeasurementReport;
import org.rhq.core.domain.measurement.MeasurementScheduleRequest;
import org.rhq.modules.plugins.jbossas7.BaseComponent;
import org.rhq.modules.plugins.jbossas7.PluginStats;
import org.rhq.modules.plugins.jbossas7.json.Operation;
import org.rhq.modules.plugins.jbossas7.json.ReadAttribute;
import org.rhq.modules.plugins.jbossas7.json.Result;

public abstract class MetricsRemappingComponent<T extends MetricsRemappingComponent<?>> extends BaseComponent<T> {
   final Log log = LogFactory.getLog(this.getClass());
   private static final String INTERNAL = "_internal:";
   private static final int INTERNAL_SIZE = INTERNAL.length();
   public static final Map<String, String> server2plugin;
   public static final Map<String, String> plugin2server;

   static {
      server2plugin = new HashMap<String, String>();
      server2plugin.put("cache-manager-status", "cacheManagerStatus");
      server2plugin.put("defined-cache-count", "definedCacheCount");
      server2plugin.put("defined-cache-names", "definedCacheNames");
      server2plugin.put("running-cache-count", "runningCacheCount");
      server2plugin.put("created-cache-count", "createdCacheCount");
      server2plugin.put("cluster-name", "clusterName");
      server2plugin.put("cluster-size", "clusterSize");
      server2plugin.put("coordinator-address", "coordinatorAddress");
      server2plugin.put("local-address", "localAddress");
      server2plugin.put("cache-status", "cacheStatus");
      server2plugin.put("cache-name", "cacheName");
      server2plugin.put("number-of-locks-available", "numberOfLocksAvailable");
      server2plugin.put("number-of-locks-held", "numberOfLocksHeld");
      server2plugin.put("concurrency-level", "concurrencyLevel");
      server2plugin.put("average-read-time", "averageReadTime");
      server2plugin.put("hit-ratio", "hitRatio");
      server2plugin.put("elapsed-time", "elapsedTime");
      server2plugin.put("read-write-ratio", "readWriteRatio");
      server2plugin.put("average-write-time", "averageWriteTime");
      server2plugin.put("average-remove-time", "averageRemoveTime");
      server2plugin.put("hits", "hits");
      server2plugin.put("evictions", "evictions");
      server2plugin.put("remove-misses", "removeMisses");
      server2plugin.put("time-since-reset", "timeSinceReset");
      server2plugin.put("number-of-entries", "numberOfEntries");
      server2plugin.put("stores", "stores");
      server2plugin.put("remove-hits", "removeHits");
      server2plugin.put("misses", "misses");
      server2plugin.put("success-ratio", "successRatio");
      server2plugin.put("replication-count", "replicationCount");
      server2plugin.put("replication-failures", "replicationFailures");
      server2plugin.put("average-replication-time", "averageReplicationTime");
      server2plugin.put("commits", "commits");
      server2plugin.put("prepares", "prepares");
      server2plugin.put("rollbacks", "rollbacks");
      server2plugin.put("invalidations", "invalidations");
      server2plugin.put("passivations", "passivations");
      server2plugin.put("activations", "activations");
      server2plugin.put("cache-loader-loads", "cacheLoaderLoads");
      server2plugin.put("cache-loader-misses", "cacheLoaderMisses");

      // we will put these 2 here as well, just to be aware of them
      server2plugin.put("bytesRead", "bytesRead");
      server2plugin.put("bytesWritten", "bytesWritten");

      plugin2server = new HashMap<String, String>(server2plugin.size());
      for (Entry<String, String> entry : server2plugin.entrySet()) {
         plugin2server.put(entry.getValue(), entry.getKey());
      }
   }

   @Override
   public void getValues(MeasurementReport report, Set<MeasurementScheduleRequest> metrics) throws Exception {

      for (MeasurementScheduleRequest req : metrics) {

         if (req.getName().startsWith(INTERNAL))
            processPluginStats(req, report);
         else {
            // Metrics from the application server

            String reqName = plugin2server.get(req.getName());
            if (reqName == null) {
               reqName = req.getName();
            }

            ComplexRequest request = null;
            Operation op;
            if (reqName.contains(":")) {
               request = ComplexRequest.create(reqName);
               op = new ReadAttribute(getAddress(), request.getProp());
            } else {
               op = new ReadAttribute(getAddress(), reqName); // TODO batching
            }

            Result res = getASConnection().execute(op);
            if (!res.isSuccess()) {
               log.warn("Getting metric [" + req.getName() + "] at [ " + getAddress() + "] failed: " + res.getFailureDescription());
               continue;
            }

            Object val = res.getResult();
            if (val == null) // One of the AS7 ways of telling "This is not implemented" See also AS7-1454
               continue;

            if (req.getDataType() == DataType.MEASUREMENT) {
               if (!val.equals("no metrics available")) { // AS 7 returns this
                  try {
                     if (request != null) {
                        HashMap<String, Number> myValues = (HashMap<String, Number>) val;
                        for (String key : myValues.keySet()) {
                           String sub = request.getSub();
                           if (key.equals(sub)) {
                              addMetric2Report(report, req, myValues.get(key));
                           }
                        }
                     } else {
                        addMetric2Report(report, req, val);
                     }
                  } catch (NumberFormatException e) {
                     log.warn("Non numeric input for [" + req.getName() + "] : [" + val + "]");
                  }
               }
            } else if (req.getDataType() == DataType.TRAIT) {

               String realVal = getStringValue(val);

               MeasurementDataTrait data = new MeasurementDataTrait(req, realVal);
               report.addData(data);
            }
         }
      }
   }

   private void processPluginStats(MeasurementScheduleRequest req, MeasurementReport report) {

      String name = req.getName();
      if (!name.startsWith(INTERNAL))
         return;

      name = name.substring(INTERNAL_SIZE);

      PluginStats stats = PluginStats.getInstance();
      MeasurementDataNumeric data;
      Double val;
      if (name.equals("mgmtRequests")) {
         val = (double) stats.getRequestCount();
      } else if (name.equals("requestTime")) {
         val = (double) stats.getRequestTime();
      } else if (name.equals("maxTime")) {
         val = (double) stats.getMaxTime();
      } else
         val = Double.NaN;

      data = new MeasurementDataNumeric(req, val);
      report.addData(data);
   }

   private void addMetric2Report(MeasurementReport report, MeasurementScheduleRequest req, Object val) {
      Double d = Double.parseDouble(getStringValue(val));
      MeasurementDataNumeric data = new MeasurementDataNumeric(req, d);
      report.addData(data);
   }

   private static class ComplexRequest {
      private String prop;
      private String sub;

      private ComplexRequest(String prop, String sub) {
         this.prop = prop;
         this.sub = sub;
      }

      public String getProp() {
         return prop;
      }

      public String getSub() {
         return sub;
      }

      public static ComplexRequest create(String requestName) {
         StringTokenizer tokenizer = new StringTokenizer(requestName, ":");
         return new ComplexRequest(tokenizer.nextToken(), tokenizer.nextToken());
      }
   }

}

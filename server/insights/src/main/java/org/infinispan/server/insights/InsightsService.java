package org.infinispan.server.insights;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Supplier;

import javax.net.ssl.SSLContext;

import com.redhat.insights.reports.InsightsReport;
import com.redhat.insights.reports.InsightsSubreport;
import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.commons.util.Version;
import org.infinispan.server.core.ServerManagement;
import org.infinispan.server.insights.logging.InsightsLoggerDelegate;
import org.infinispan.server.insights.logging.Log;
import org.infinispan.server.insights.report.InfinispanSubreport;
import org.infinispan.server.insights.report.InfinispanTopReport;
import org.infinispan.server.insights.scheduler.InfinispanInsightsScheduler;
import org.infinispan.util.concurrent.BlockingManager;

import com.redhat.insights.Filtering;
import com.redhat.insights.InsightsReportController;
import com.redhat.insights.config.InsightsConfiguration;
import com.redhat.insights.core.httpclient.InsightsJdkHttpClient;
import com.redhat.insights.http.InsightsFileWritingClient;
import com.redhat.insights.http.InsightsMultiClient;
import com.redhat.insights.jars.ClasspathJarInfoSubreport;
import com.redhat.insights.logging.InsightsLogger;
import com.redhat.insights.tls.PEMSupport;

public class InsightsService {

   private static final Log log = LogFactory.getLog(InsightsService.class, Log.class);

   private final ServerManagement server;
   private final InsightsConfiguration config;
   private final InsightsLogger insightsLogger;
   private final InsightsReport insightsReport;

   private InsightsReportController insightsReportController;
   private volatile String identificationName;

   public InsightsService(ServerManagement server) {
      this.server = server;
      this.config = new InfinispanInsightsConfiguration(this::identificationName);
      this.insightsLogger = new InsightsLoggerDelegate(log);

      if (config.isOptingOut()) {
         throw log.insightsConfigurationError();
      }

      Map<String, InsightsSubreport> subReports = new LinkedHashMap<>(2);
      InfinispanSubreport infinispanSubreport = new InfinispanSubreport(this::overviewReport);
      ClasspathJarInfoSubreport jarsSubreport = new ClasspathJarInfoSubreport(insightsLogger);
      subReports.put("infinispan", infinispanSubreport);
      subReports.put("jars", jarsSubreport);
      insightsReport = new InfinispanTopReport(insightsLogger, config, this::identificationName, subReports);
   }

   public void start(BlockingManager blockingManager) {
      PEMSupport pemSupport = new PEMSupport(insightsLogger, config);
      Supplier<SSLContext> sslContextSupplier = () -> {
         try {
            return pemSupport.createTLSContext();
         } catch (Throwable ex) {
            log.insightsCertificateError();
            throw ex;
         }
      };

      InfinispanInsightsScheduler insightsScheduler =
            new InfinispanInsightsScheduler(insightsLogger, config, blockingManager);

      try {
         insightsReportController = InsightsReportController.of(insightsLogger, config, insightsReport,
               () -> new InsightsMultiClient(insightsLogger,
                     new InsightsJdkHttpClient(insightsLogger, config, sslContextSupplier),
                     new InsightsFileWritingClient(insightsLogger, config)), insightsScheduler,
               new LinkedBlockingQueue<>());
         insightsReportController.generate();
      } catch (Throwable ex) {
         throw log.insightsServiceSetupError(ex);
      }
   }

   public void stop() {
      if (insightsReportController != null) {
         insightsReportController.shutdown();
      }
   }

   public InsightsReport report() {
      insightsReport.generateReport(Filtering.DEFAULT);
      return insightsReport;
   }

   public String identificationName() {
      if (identificationName == null) {
         overviewReport();
      }
      return identificationName;
   }

   private Json overviewReport() {
      Json report = server.overviewReport();
      if (identificationName != null) {
         return report;
      }

      if (log.isDebugEnabled()) {
         log.debug("Starting Insights integration. Current Infinispan report: \n\r" + report.toPrettyString());
      }
      Json nodeIdReport = report.at("node-id");
      String nodeId = (nodeIdReport.isNull()) ? UUID.randomUUID().toString() : nodeIdReport.asString();
      identificationName = Version.getBrandName() + " " + nodeId;
      return report;
   }
}

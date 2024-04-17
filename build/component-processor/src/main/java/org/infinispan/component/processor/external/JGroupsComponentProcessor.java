package org.infinispan.component.processor.external;

import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.Processor;
import javax.annotation.processing.RoundEnvironment;
import javax.annotation.processing.SupportedAnnotationTypes;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.Element;
import javax.lang.model.element.TypeElement;
import javax.lang.model.type.DeclaredType;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.infinispan.external.JGroupsProtocolComponent;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.protocols.FlowControl;
import org.jgroups.protocols.MsgStats;
import org.jgroups.protocols.RED;
import org.jgroups.protocols.TP;
import org.jgroups.stack.Protocol;
import org.jgroups.util.ThreadPool;
import org.kohsuke.MetaInfServices;

/**
 * @since 14.0
 **/
@SupportedAnnotationTypes("org.infinispan.external.JGroupsProtocolComponent")
@MetaInfServices(Processor.class)
public class JGroupsComponentProcessor extends AbstractProcessor {

   private static final Map<String, String> FLOW_CONTROL_METRICS = Map.of(
         "getNumberOfCreditRequestsReceived", "Number of credit requests received",
         "getNumberOfCreditRequestsSent", "Number of credit requests sent",
         "getNumberOfCreditResponsesReceived", "Number of credit responses received",
         "getNumberOfCreditResponsesSent", "Number of credit responses sent"
   );

   private static final List<JgrpMetric> MSG_STATS_METRICS = List.of(
         new JgrpMetric("getNumBatchesSent", "Number of message batches sent"),
         new JgrpMetric("getNumBatchesReceived", "Number of message batches received"),
         new JgrpMetric("getNumMcastBytesReceived", "Number of multicast bytes received"),
         new JgrpMetric("getNumMcastBytesSent", "Number of multicast bytes sent"),
         new JgrpMetric("getNumMcastMsgsReceived", "getNumMcastsReceived", "Number of multicast messages received"),
         new JgrpMetric("getNumMcastMsgsSent", "getNumMcastsSent", "getNumMcastMsgsSent"),
         new JgrpMetric("getNumSingleMsgsSent", "Number of single messages sent"),
         new JgrpMetric("getNumUcastBytesReceived", "Number of unicast bytes received"),
         new JgrpMetric("getNumUcastBytesSent", "Number of unicast bytes sent"),
         new JgrpMetric("getNumUcastMsgsReceived", "getNumUcastsReceived", "Number of unicast messages received"),
         new JgrpMetric("getNumUcastMsgsSent", "getNumUcastsSent", "Number of unicast messages sent")
   );

   private static final List<JgrpMetric> THREAD_POOL_METRICS = List.of(
         new JgrpMetric("getNumRejectedMsgs", "numberOfRejectedMessages", "Number of dropped messages that were rejected by the thread pool")
   );

   @Override
   public SourceVersion getSupportedSourceVersion() {
      return SourceVersion.latestSupported();
   }

   @Override
   public boolean process(Set<? extends TypeElement> annotations, RoundEnvironment roundEnv) {
      for (TypeElement annotation : annotations) {
         for (Element element : roundEnv.getElementsAnnotatedWith(annotation)) {
            JGroupsProtocolComponent component = element.getAnnotation(JGroupsProtocolComponent.class);
            DeclaredType typeMirror = (DeclaredType) element.asType();
            TypeElement type = (TypeElement) typeMirror.asElement();
            String fqcn = type.getQualifiedName().toString();
            int lastDot = fqcn.lastIndexOf('.');
            String packageName = fqcn.substring(0, lastDot);
            try (PrintWriter w = new PrintWriter(processingEnv.getFiler().createSourceFile(packageName + '.' + component.value()).openWriter())) {
               generate(w, packageName, component.value());
            } catch (IOException e) {
               throw new RuntimeException(e);
            }
         }
      }

      return true;
   }

   public void generate(PrintWriter w, String packageName, String className) throws IOException {
      w.printf("package %s;%n", packageName);
      w.println("import java.util.ArrayList;");
      w.println("import java.util.Collection;");
      w.println("import java.util.HashMap;");
      w.println("import java.util.List;");
      w.println("import java.util.Map;");
      w.println("import java.util.function.Function;");
      w.println("import javax.annotation.processing.Generated;");
      w.println("import org.infinispan.commons.stat.GaugeMetricInfo;");
      w.println("import org.infinispan.commons.stat.MetricInfo;");
      w.println("import org.jgroups.stack.Protocol;");
      w.println();
      w.printf("@Generated(value = \"%s\", date = \"%s\")%n", getClass().getName(), Instant.now().toString());
      w.printf("public class %s {%n", className);
      w.println("   public static final Map<Class<? extends Protocol>, Collection<MetricInfo>> PROTOCOL_METADATA = new HashMap<>();");
      w.printf("   private %s() {}%n", className);
      w.println("   static {");
      w.println("      List<MetricInfo> attributes;");
      for (short id = 0; id < 256; id++) {
         Class<?> protocol = ClassConfigurator.getProtocol(id);
         addProtocol(protocol, w);
      }

      // RED protocol does not have an ID
      // Reason: protocol that does not send headers around does not need an ID.
      // Add it manually if an ID is not found.
      if (ClassConfigurator.getProtocolId(RED.class) == 0) {
         addProtocol(RED.class, w);
      }

      w.println("   }");
      w.println("}");
      w.println();
      w.flush();
      w.close();
   }

   private static void addProtocol(Class<?> protocol, PrintWriter w) {
      if (protocol == null || !Protocol.class.isAssignableFrom(protocol) || Modifier.isAbstract(protocol.getModifiers())) {
         return;
      }
      AtomicBoolean hasAttributes = new AtomicBoolean(false);

      for (Method method : protocol.getMethods()) {
         ManagedAttribute annotation = method.getAnnotation(ManagedAttribute.class);
         if (annotation != null && !Modifier.isStatic(method.getModifiers()) && method.getParameterCount() == 0 && isNumber(method.getReturnType())) {
            if (hasAttributes.compareAndSet(false, true)) {
               w.println("      attributes = new ArrayList<>();");
            }
            w.printf("      attributes.add(new GaugeMetricInfo<>(\"%s\", \"%s\", null, %s::%s));%n",
                  method.getName(), annotation.description().replace('"', '\''), protocol.getName(), method.getName());
         }
      }

      if (TP.class.isAssignableFrom(protocol)) {
         addThreadPoolComponent(protocol, w, hasAttributes);
         addMsgStatsComponent(protocol, w, hasAttributes);
      }

      if (FlowControl.class.isAssignableFrom(protocol)) {
         addFlowControlMetrics(protocol, w, hasAttributes);
      }

      if (hasAttributes.get()) {
         // only put the protocol in PROTOCOL_METADATA if we have attributes available
         w.printf("      PROTOCOL_METADATA.put(%s.class, attributes);%n", protocol.getName());
      }
   }

   private static void addThreadPoolComponent(Class<?> protocol, PrintWriter w, AtomicBoolean hasAttributes) {
      addTPComponent("getThreadPool", protocol, ThreadPool.class, w, hasAttributes);
      if (hasAttributes.compareAndSet(false, true)) {
         w.println("      attributes = new ArrayList<>();");
      }
      THREAD_POOL_METRICS.forEach(metric -> w.printf("      attributes.add(new GaugeMetricInfo<>(\"%s\", \"%s\", null, ((Function<%s, Number>) p -> p.getThreadPool().%s())));%n",
            metric.metric, metric.description.replace('"', '\''), protocol.getName(), metric.method));
   }


   private static void addMsgStatsComponent(Class<?> protocol, PrintWriter w, AtomicBoolean hasAttributes) {
      addTPComponent("getMessageStats", protocol, MsgStats.class, w, hasAttributes);
      if (hasAttributes.compareAndSet(false, true)) {
         w.println("      attributes = new ArrayList<>();");
      }
      MSG_STATS_METRICS.forEach(metric -> w.printf("      attributes.add(new GaugeMetricInfo<>(\"%s\", \"%s\", null, ((Function<%s, Number>) p -> p.getMessageStats().%s())));%n",
            metric.metric, metric.description.replace('"', '\''), protocol.getName(), metric.method));
   }

   private static void addFlowControlMetrics(Class<?> protocol, PrintWriter w, AtomicBoolean hasAttributes) {
      if (hasAttributes.compareAndSet(false, true)) {
         w.println("      attributes = new ArrayList<>();");
      }
      FLOW_CONTROL_METRICS.forEach((method, description) -> w.printf("      attributes.add(new GaugeMetricInfo<>(\"%s\", \"%s\", null, %s::%s));%n",
            method, description.replace('"', '\''), protocol.getName(), method));
   }

   private static boolean isNumber(Class<?> type) {
      return short.class == type || byte.class == type || long.class == type || int.class == type || float.class == type || double.class == type || Number.class.isAssignableFrom(type);
   }

   private static void addTPComponent(String getterMethodName, Class<?> protocol, Class<?> component, PrintWriter w, AtomicBoolean hasAttributes) {
      for (Method method : component.getMethods()) {
         ManagedAttribute annotation = method.getAnnotation(ManagedAttribute.class);
         if (annotation != null && !Modifier.isStatic(method.getModifiers()) && method.getParameterCount() == 0 && isNumber(method.getReturnType())) {
            if (hasAttributes.compareAndSet(false, true)) {
               w.println("      attributes = new ArrayList<>();");
            }
            w.printf("      attributes.add(new GaugeMetricInfo<>(\"%s\", \"%s\", null, ((Function<%s, Number>) p -> p.%s().%s())));%n",
                  method.getName(), annotation.description().replace('"', '\''), protocol.getName(), getterMethodName, method.getName());
         }
      }
   }

   private static class JgrpMetric {
      final String metric;
      final String method;
      final String description;

      JgrpMetric(String metric, String method, String description) {
         this.metric = metric;
         this.method = method;
         this.description = description;
      }

      JgrpMetric(String metric, String description) {
         this(metric, metric, description);
      }
   }
}

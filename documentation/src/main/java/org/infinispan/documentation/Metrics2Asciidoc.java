package org.infinispan.documentation;

import java.io.IOException;
import java.io.PrintStream;
import java.lang.reflect.AccessibleObject;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import org.infinispan.commons.configuration.io.NamingStrategy;
import org.infinispan.commons.stat.MetricInfo;
import org.infinispan.commons.util.ServiceFinder;
import org.infinispan.factories.impl.ComponentAccessor;
import org.infinispan.factories.impl.MBeanMetadata;
import org.infinispan.factories.impl.ModuleMetadataBuilder;
import org.infinispan.factories.impl.Scopes;
import org.infinispan.remoting.transport.jgroups.JGroupsMetricsMetadata;
import org.jgroups.annotations.MBean;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.protocols.RED;
import org.jgroups.protocols.TP;
import org.jgroups.stack.Protocol;

public class Metrics2Asciidoc {
   public static void main(String[] args) throws IOException {
      Path path = Path.of(args[0]);
      Path parent = path.getParent();
      if (parent != null) {
         Files.createDirectories(parent);
      }
      try (PrintStream out = new PrintStream(Files.newOutputStream(path))) {
         modulesMetrics(out);
         jgroupsMetrics(out);
      } catch (IOException e) {
         throw new RuntimeException(e);
      }
   }

   private static void modulesMetrics(PrintStream out) {
      Collection<ModuleMetadataBuilder> builders = ServiceFinder.load(ModuleMetadataBuilder.class);
      AtomicReference<String> currentModule = new AtomicReference<>();
      AtomicReference<Scopes> currentType = new AtomicReference<>();
      AtomicReference<String> currentObject = new AtomicReference<>();
      Set<String> seen = new HashSet<>();

      // We cycle through all the modules
      for (ModuleMetadataBuilder b : builders) {
         // We cycle through all the scopes so that we group metrics based on them
         for (Scopes scope : Scopes.values()) {
            b.registerMetadata(new ModuleMetadataBuilder.ModuleBuilder() {
               @Override
               public void registerMBeanMetadata(String componentClassName, MBeanMetadata metadata) {
                  String objectName = metadata.getJmxObjectName();
                  if (metadata.scope() == scope && objectName != null && !metadata.getAttributes().isEmpty()) {
                     String moduleName = b.getModuleName();
                     if (!moduleName.equals(currentModule.get())) {
                        header(out, 1, moduleName);
                        currentModule.set(moduleName);
                        currentType.set(null);
                        currentObject.set(null);
                     }
                     Scopes scope = metadata.scope();
                     if (!scope.equals(currentType.get())) {
                        header(out, 2, scopeName(scope));
                        currentType.set(scope);
                        currentObject.set(null);
                     }
                     if (!objectName.equals(currentObject.get())) {
                        header(out, 3, objectName);
                        out.println(metadata.getDescription());
                        currentObject.set(objectName);
                     }
                     processMetadata(metadata);
                  }
               }

               private void processMetadata(MBeanMetadata mBeanMetadata) {
                  String cName = NamingStrategy.SNAKE_CASE.convert(mBeanMetadata.getJmxObjectName());
                  for (MBeanMetadata.AttributeMetadata attr : mBeanMetadata.getAttributes()) {
                     String key = mBeanMetadata.getJmxObjectName() + '.' + attr.getName();
                     if (!seen.contains(key)) {
                        out.println();
                        header(out, 4, attr.getName());
                        out.printf("_%s_%n", attr.getDescription());
                        tableHeader(out, "Environment", "Location", "Type");
                        attr.toMetricInfo().ifPresent(m -> {
                           out.printf("| OpenMetrics | `infinispan_%s{%scache_manager=\"${cache_manager}\",node=\"${node}\"}` | %s%n", NamingStrategy.SNAKE_CASE.convert(m.getName()), mBeanMetadata.scope() == Scopes.GLOBAL ? "" : "cache=\"${cache}\",", m.getType());
                        });
                        out.printf("| JMX | ObjectName: `org.infinispan:type=%s,name=\"%s\",component=%s` +%n Attribute: `%s`| %s%n",
                              mBeanMetadata.scope() == Scopes.GLOBAL ? "CacheManager" : "Cache",
                              mBeanMetadata.scope() == Scopes.GLOBAL ? "${cache_manager}" : "${cache}",
                              mBeanMetadata.getJmxObjectName(), attr.getName(), simpleType(attr.getType()));
                        tableFooter(out);
                        seen.add(key);
                     }
                  }

               }

               @Override
               public void registerComponentAccessor(String componentClassName, List<String> factoryComponentNames, ComponentAccessor<?> accessor) {
               }

               @Override
               public String getFactoryName(String componentName) {
                  return "";
               }
            });
         }
      }
   }

   private static String simpleType(String type) {
      int i = type.lastIndexOf('.');
      return i < 0 ? type : type.substring(i + 1);
   }

   private static void jgroupsMetrics(PrintStream out) {
      header(out, 1, "JGroups");
      List<Class<? extends Protocol>> protocols = new ArrayList<>();
      for (short id = 0; id < 256; id++) {
         Class<? extends Protocol> protocol = (Class<? extends Protocol>) ClassConfigurator.getProtocol(id);
         if (protocol != null) {
            protocols.add(protocol);
         }
      }
      protocols.add(RED.class);
      protocols.sort(Comparator.comparing(Class::getSimpleName));

      for (Class<? extends Protocol> protocol : protocols) {
         header(out, 2, protocol.getSimpleName());
         MBean mBean = protocol.getAnnotation(MBean.class);
         if (mBean == null) continue;
         out.println(mBean.description());

         Map<String, AccessibleObject> all = new TreeMap<>();
         all.putAll(Arrays.stream(protocol.getMethods()).filter(m -> m.isAnnotationPresent(ManagedAttribute.class) && !m.isAnnotationPresent(Deprecated.class)).collect(Collectors.toMap(Method::getName, m -> m)));
         all.putAll(Arrays.stream(protocol.getDeclaredFields()).filter(f -> f.isAnnotationPresent(ManagedAttribute.class) && !f.isAnnotationPresent(Deprecated.class)).collect(Collectors.toMap(Field::getName, f -> f)));
         if (TP.class.isAssignableFrom(protocol)) {
            all.putAll(Arrays.stream(TP.class.getMethods()).filter(m -> m.isAnnotationPresent(ManagedAttribute.class) && !m.isAnnotationPresent(Deprecated.class)).collect(Collectors.toMap(m -> "thread_pool." + m.getName(), m -> m)));
            all.putAll(Arrays.stream(TP.class.getDeclaredFields()).filter(f -> f.isAnnotationPresent(ManagedAttribute.class) && !f.isAnnotationPresent(Deprecated.class)).collect(Collectors.toMap(f -> "thread_pool." + f.getName(), f -> f)));
         }
         Collection<MetricInfo> protocolMetrics = JGroupsMetricsMetadata.PROTOCOL_METADATA.get(protocol);
         Map<String, MetricInfo> metrics = protocolMetrics == null ? Collections.emptyMap() : protocolMetrics.stream().collect(Collectors.toMap(MetricInfo::getName, m -> m));

         for (Map.Entry<String, AccessibleObject> item : all.entrySet()) {
            AccessibleObject o = item.getValue();
            ManagedAttribute attribute = o.getAnnotation(ManagedAttribute.class);
            out.println();
            String name = o instanceof Field ? ((Field) o).getName() : ((Method) o).getName();
            String type = o instanceof Field ? ((Field) o).getType().getSimpleName() : ((Method) o).getReturnType().getSimpleName();
            header(out, 3, name);
            out.printf("_%s_%n", attribute.description());
            tableHeader(out, "Environment", "Location", "Type");
            MetricInfo m = metrics.get(name);
            if (m != null) {
               out.printf("| OpenMetrics | `jgroups_%s{cache_manager=\"${cache_manager}\",cluster=\"${cluster}\",node=\"${node}\"}` | %s%n", NamingStrategy.SNAKE_CASE.convert(m.getName()), m.getType());
            }
            out.printf("| JMX | ObjectName: `org.infinispan:manager=\"${cache_manager}\",type=protocol,cluster=\"${cluster}\",protocol=%s` +%nAttributeName: `%s` | %s%n",
                  protocol.getSimpleName(), NamingStrategy.SNAKE_CASE.convert(item.getKey()), simpleType(type));
            tableFooter(out);
         }
      }
   }

   private static void header(PrintStream out, int level, String title) {
      out.println();
      out.printf("%s %s%n", "=".repeat(level), NamingStrategy.TITLE_CASE.convert(title));
   }

   private static void tableHeader(PrintStream out, String... columns) {
      out.printf("""
            [width=100%%]
            |===
            |%s%n
            """, String.join("|", columns));
   }

   private static void tableFooter(PrintStream out) {
      out.println("|===");
   }

   private static String scopeName(Scopes scope) {
      return switch (scope) {
         case GLOBAL -> "Cache Manager";
         case NAMED_CACHE -> "Cache";
         case NONE -> "N/A";
         case SERVER -> "Server";
      };
   }

}

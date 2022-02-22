package org.infinispan.logging.processor;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.Processor;
import javax.annotation.processing.RoundEnvironment;
import javax.annotation.processing.SupportedAnnotationTypes;
import javax.annotation.processing.SupportedSourceVersion;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.Element;
import javax.lang.model.element.TypeElement;
import javax.tools.Diagnostic;
import javax.tools.StandardLocation;
import javax.xml.stream.XMLStreamException;

import org.infinispan.logging.processor.report.XmlReportWriter;
import org.kohsuke.MetaInfServices;
/**
 * InfinispanLoggingProcessor.
 *
 * @author Durgesh Anaokar
 * @since 13.0
 */
@SupportedSourceVersion(SourceVersion.RELEASE_8)
@SupportedAnnotationTypes({InfinispanLoggingProcessor.DESCRIPTION})
@MetaInfServices(Processor.class)
public class InfinispanLoggingProcessor extends AbstractProcessor {
   static final String DESCRIPTION = "org.infinispan.logging.annotations.Description";

   @Override
   public boolean process(Set<? extends TypeElement> annotations, RoundEnvironment roundEnv) {
      for (TypeElement annotation : annotations) {
         createReportXml(roundEnv, annotation);
      }
      return true;
   }

   private void createReportXml(RoundEnvironment roundEnv, TypeElement annotation) {
      Set<? extends Element> annotatedElements = roundEnv.getElementsAnnotatedWith(annotation);
      Map<String, List<Element>> mapWithQualifiedName = getMapWithQualifiedName(annotatedElements);
      for (Map.Entry<String, List<Element>> entry : mapWithQualifiedName.entrySet()) {
         Element e = entry.getValue().get(0);
         String qualifiedName = e.getEnclosingElement().toString();
         Element enclosing = e.getEnclosingElement();
         String packageName = processingEnv.getElementUtils().getPackageOf(enclosing).getQualifiedName().toString();
         String simpleName = enclosing.getSimpleName().toString();
         try (BufferedWriter bufferedWriter = createWriter(packageName, simpleName + ".xml");
            XmlReportWriter reportWriter = new XmlReportWriter(bufferedWriter)) {
            reportWriter.writeHeader(qualifiedName);
            for (Element element : entry.getValue()) {
               reportWriter.writeDetail(element);
            }
            reportWriter.writeFooter();
         } catch (XMLStreamException | IOException ex) {
            error(e, "Error encountered when writing the report: " + ex.getMessage(), e);
            break;
         }
      }
   }

   private BufferedWriter createWriter(final String packageName, final String fileName) throws IOException {
      return new BufferedWriter(
            processingEnv.getFiler().createResource(StandardLocation.CLASS_OUTPUT, packageName, fileName).openWriter());
   }

   private void error(Element e, String format, Object... params) {
      String formatted = String.format(format, params);
      if (e != null) {
         processingEnv.getMessager().printMessage(Diagnostic.Kind.ERROR, formatted, e);
      } else {
         processingEnv.getMessager().printMessage(Diagnostic.Kind.ERROR, formatted);
      }
   }

   private Map<String, List<Element>> getMapWithQualifiedName(Set<? extends Element> annotatedElements) {
      Map<String, List<Element>> elementMap = new HashMap<>();
      for (Element interfaceElement : annotatedElements) {
         List<Element> list = elementMap.get(interfaceElement.getEnclosingElement().toString());
         if (list != null) {
            list.add(interfaceElement);
         } else {
            list = new LinkedList<Element>();
            list.add(interfaceElement);
            elementMap.put(interfaceElement.getEnclosingElement().toString(), list);
         }
      }
      return elementMap;
   }
}

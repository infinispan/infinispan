package org.infinispan.tools.licenses;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.TransformerException;

import org.infinispan.tools.ToolUtils;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;

import gnu.getopt.Getopt;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class LicenseMerger {
   private final DocumentBuilder docBuilder;
   private final Map<String, Document> xmls = new LinkedHashMap<>();
   private final Document emptyDocument;
   private boolean verbose;

   LicenseMerger() throws Exception {
      DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
      dbf.setNamespaceAware(true);
      docBuilder = dbf.newDocumentBuilder();
      emptyDocument = docBuilder.newDocument();
   }

   private void setVerbose(boolean verbose) {
      this.verbose = verbose;
   }

   private void loadLicenseFromJar(String fileName) throws Exception {
      try (JarFile jar = new JarFile(fileName)) {
         JarEntry entry = jar.getJarEntry("META-INF/licenses.xml");
         if (entry != null) {
            try (InputStream inputStream = jar.getInputStream(entry)) {
               Document doc = docBuilder.parse(inputStream);
               xmls.put(ToolUtils.getBaseFileName(fileName), doc);
               if (verbose) {
                  System.out.printf("Loaded license from JAR %s\n", fileName);
               }
            }
         } else {
            xmls.put(ToolUtils.getBaseFileName(fileName), emptyDocument);
            if (verbose) {
               System.out.printf("Empty license for JAR %s\n", fileName);
            }
         }
      }
   }

   private void loadLicenseFromXML(String fileName) throws Exception {
      Document doc = docBuilder.parse(new File(fileName));
      xmls.put(ToolUtils.getBaseFileName(fileName), doc);
      if (verbose) {
         System.out.printf("Loaded license from XML %s\n", fileName);
      }
   }

   void loadLicense(String filename) throws Exception {
      if (filename.endsWith(".jar"))
         loadLicenseFromJar(filename);
      else if (filename.endsWith(".xml"))
         loadLicenseFromXML(filename);
      else
         throw new IllegalArgumentException(filename);
   }

   Node findFirstChildByTagName(Node parent, String tagName) {
      for(Node child = parent.getFirstChild(); child != null; ) {
         if (tagName.equals(child.getLocalName())) {
            return child;
         }
         child = child.getNextSibling();
      }
      return null;
   }

   public void write(boolean inclusiveMode, OutputStream os) throws IOException, TransformerException {
      if (verbose) {
         System.out.printf("Inclusive mode %s\n", Boolean.toString(inclusiveMode));
      }
      Document aggregated = docBuilder.newDocument();
      Element aggregatedDependencies = (Element) aggregated
            .appendChild(aggregated.createElement("licenseSummary"))
            .appendChild(aggregated.createElement("dependencies"));
      Map<String, Node> artifacts = new ConcurrentHashMap<>();
      // Loop through all of the collected licenses.xml files
      for (Map.Entry<String, Document> l : xmls.entrySet()) {
         Document doc = l.getValue();
         if (doc == emptyDocument) continue;
         Node dependencies = doc.getElementsByTagName("dependencies").item(0);
         for(Node dependency = dependencies.getFirstChild(); dependency != null; ) {
            if ("dependency".equals(dependency.getLocalName())) {
               String groupId = findFirstChildByTagName(dependency, "groupId").getTextContent().trim();
               String artifactId = findFirstChildByTagName(dependency, "artifactId").getTextContent().trim();
               String version = findFirstChildByTagName(dependency, "version").getTextContent().trim();
               // Only include artifact if not in inclusive mode or if it's one of the included jars
               if (!inclusiveMode || xmls.containsKey(String.format("%s-%s", artifactId, version))) {
                  String artifact = String.format("%s:%s", groupId, artifactId);
                  final Node dep = dependency;
                  artifacts.computeIfAbsent(artifact, a -> aggregated.adoptNode(dep.cloneNode(true)));
               }
            }
            dependency = dependency.getNextSibling();
         }
      }
      artifacts.entrySet().stream()
            .sorted(Map.Entry.comparingByKey())
            .forEach(entry ->  aggregatedDependencies.appendChild(entry.getValue()));
      ToolUtils.printDocument(aggregated, os);

   }

   public static void main(String argv[]) throws Exception {
      LicenseMerger licenseMerger = new LicenseMerger();
      File outputFile = new File(System.getProperty("user.dir"), "licenses.xml");
      boolean inclusiveMode = false;
      boolean verbose = false;
      Getopt opts = new Getopt("license-merger", argv, "vio:r:");
      for (int opt = opts.getopt(); opt > -1; opt = opts.getopt()) {
         switch (opt) {
            case 'v':
               verbose = true;
               licenseMerger.setVerbose(true);
               break;
            case 'i':
               inclusiveMode = true;
               break;
            case 'o':
               outputFile = new File(opts.getOptarg());
               break;
            case 'r':
               String[] responseData = new String(Files.readAllBytes(Paths.get(opts.getOptarg())), StandardCharsets.UTF_8).split("\\s+");
               for(String filename : responseData) {
                  licenseMerger.loadLicense(filename);
               }
               break;
         }
      }
      outputFile.getParentFile().mkdirs();
      for (int i = opts.getOptind(); i < argv.length; i++) {
         licenseMerger.loadLicense(argv[i]);
      }
      try (OutputStream os = new FileOutputStream(outputFile)) {
         licenseMerger.write(inclusiveMode, os);
         if (verbose) {
            System.out.printf("Wrote merged licenses to %s\n",  outputFile);
         }
      }
   }
}

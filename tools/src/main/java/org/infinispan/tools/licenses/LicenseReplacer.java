package org.infinispan.tools.licenses;

import static java.lang.System.exit;
import static org.infinispan.tools.ToolUtils.EMPTY;
import static org.infinispan.tools.ToolUtils.findFirstChildByPath;
import static org.infinispan.tools.ToolUtils.findFirstChildByTagName;
import static org.infinispan.tools.ToolUtils.parseXMLDependencies;
import static org.infinispan.tools.ToolUtils.removeEmptyLinesFromFile;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.TransformerException;

import org.infinispan.tools.Dependency;
import org.infinispan.tools.ToolUtils;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.xml.sax.SAXException;

import gnu.getopt.Getopt;

/**
 * Looks for "unknown" or "empty" license information and replaces.
 * <p>
 * It reads an XML file with the licenses (usually manually created) and replaces all the "unknown" or "missing"
 * licenses information from an XMl licenses file. It does not touch entries with correct license information.
 *
 * @author Pedro Ruivo
 * @since 12.0
 **/
public class LicenseReplacer {
   private final DocumentBuilder docBuilder;
   private final Document emptyDocument;
   private final Map<String, Node> overwriteArtifacts = new ConcurrentHashMap<>();
   private Document licensesDoc;
   private boolean verbose;

   LicenseReplacer() throws Exception {
      DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
      dbf.setNamespaceAware(true);
      docBuilder = dbf.newDocumentBuilder();
      emptyDocument = docBuilder.newDocument();
   }

   public static void main(String[] argv) throws Exception {
      LicenseReplacer replacer = new LicenseReplacer();
      File outputFile = new File(System.getProperty("user.dir"), "licenses.xml");
      Getopt opts = new Getopt("license-replacer", argv, "vl:i:o:");
      for (int opt = opts.getopt(); opt > -1; opt = opts.getopt()) {
         switch (opt) {
            case 'v':
               replacer.setVerbose();
               break;
            case 'i':
               replacer.loadOverwriteXML(opts.getOptarg());
               break;
            case 'o':
               outputFile = new File(opts.getOptarg());
               break;
            case 'l':
               replacer.loadLicenseFromXML(opts.getOptarg());
               break;
         }
      }
      if (replacer.licensesDoc == null || replacer.licensesDoc == replacer.emptyDocument) {
         System.err.println("License XML file is invalid or missing. Did you use '-l' option?");
         exit(1);
      }
      if (!outputFile.getParentFile().exists() && !outputFile.getParentFile().mkdirs()) {
         System.err.printf("Unable to create output file \"%s\"%n", outputFile.getAbsolutePath());
         exit(2);
      }
      if (replacer.overwriteArtifacts.isEmpty()) {
         System.err.println("Licenses overwrite XML file is empty or missing! Did you use '-i' option?");
         exit(3);
      }
      try (OutputStream os = new FileOutputStream(outputFile)) {
         replacer.write(os);
         if (replacer.verbose) {
            System.out.printf("Wrote merged licenses to %s%n", outputFile);
         }
      }
      removeEmptyLinesFromFile(outputFile);
   }

   public void write(OutputStream os) throws TransformerException {
      Document aggregated = docBuilder.newDocument();
      Element aggregatedDependencies = (Element) aggregated
            .appendChild(aggregated.createElement("licenseSummary"))
            .appendChild(aggregated.createElement("dependencies"));

      List<Node> childs = new LinkedList<>();
      if (licensesDoc != null && licensesDoc != emptyDocument) {
         for (Dependency dep : parseXMLDependencies(licensesDoc)) {
            Node depNode = aggregated.adoptNode(dep.getNode().cloneNode(true));
            String licenseName = findFirstChildByPath(depNode, "licenses/license/name")
                  .map(ToolUtils::textFromNode)
                  .orElse(EMPTY);
            if (licenseName.isEmpty() || "unknown".equalsIgnoreCase(licenseName)) {
               Node overwriteNode = overwriteArtifacts.get(dep.getArtifact());
               if (overwriteNode != null) {
                  if (verbose) {
                     System.out.printf("Overwriting license information for \"%s\"%n", dep.getArtifact());
                  }
                  findFirstChildByTagName(depNode, "licenses")
                        .ifPresent(depNode::removeChild);
                  findFirstChildByTagName(overwriteNode, "licenses")
                        .ifPresent(node -> depNode.appendChild(aggregated.adoptNode(node.cloneNode(true))));
               }
            }
            childs.add(depNode);
         }
      }
      childs.forEach(aggregatedDependencies::appendChild);
      ToolUtils.printDocument(aggregated, os);

   }

   void loadOverwriteXML(String fileName) throws IOException, SAXException {
      System.out.printf("Loading XML with overwrites from \"%s\"%n", fileName);
      Document doc = docBuilder.parse(new File(fileName));
      if (doc == emptyDocument) {
         System.err.printf("File \"%s\" is empty!%n", fileName);
      }
      ToolUtils.parseXMLDependencies(doc)
            .forEach(deo -> {
               overwriteArtifacts.put(deo.getArtifact(), deo.getNode());
               if (verbose) {
                  System.out.printf("Found artifact %s to overwrite.%n", deo.getArtifact());
               }
            });
   }

   private void setVerbose() {
      this.verbose = true;
   }

   private void loadLicenseFromXML(String fileName) throws Exception {
      System.out.printf("Loading licenses from XML \"%s\"%n", fileName);
      licensesDoc = docBuilder.parse(new File(fileName));
      if (licensesDoc == emptyDocument) {
         System.err.printf("File \"%s\" is empty!%n", fileName);
      }
   }
}

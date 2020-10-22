package org.infinispan.tools.xml;

import java.io.PrintWriter;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathFactory;

import org.w3c.dom.DOMException;
import org.w3c.dom.Document;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import gnu.getopt.Getopt;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 12.0
 **/
public class XmlConfigFlattener {
   private final DocumentBuilder docBuilder;
   private boolean verbose;

   XmlConfigFlattener() throws Exception {
      DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
      dbf.setNamespaceAware(true);
      dbf.setXIncludeAware(true);
      dbf.setIgnoringComments(false);
      docBuilder = dbf.newDocumentBuilder();
   }

   private void setVerbose(boolean verbose) {
      this.verbose = verbose;
   }

   private void flatten(Path source, Path outputDir) throws Exception {
      Document doc = docBuilder.parse(source.toFile());
      Node root = doc.getFirstChild();

      // Remove unwanted schema attributes and empty nodes
      filterNode(root);

      // Remove the test jgroups stacks
      XPath xp = XPathFactory.newInstance().newXPath();
      NodeList stacks = (NodeList) xp.compile("/*/*/*[local-name()='stack' and contains(@name, 'test-')]").evaluate(doc, XPathConstants.NODESET);
      for (int i = stacks.getLength() - 1; i >= 0; i--) {
         Node item = stacks.item(i);
         item.getParentNode().removeChild(item);
      }

      // Remove the jgroups stack if empty
      Node jgroups = (Node) xp.compile("/*/*[local-name()='jgroups']").evaluate(doc, XPathConstants.NODE);
      if (!jgroups.hasChildNodes()) {
         jgroups.getParentNode().removeChild(jgroups);
      }

      String filename = source.getFileName().toString().replaceAll("Test", "");
      Path target = outputDir.resolve(filename);

      try (Writer w = Files.newBufferedWriter(target)) {
         prettyXml(doc, w);
      }
      if (verbose) {
         System.out.printf("%s\n", target);
         prettyXml(doc, new PrintWriter(System.out));
      }
   }

   private void removeUnwantedAttributes(Node node) {
      NamedNodeMap attributes = node.getAttributes();
      removeAttribute(attributes, "http://www.w3.org/XML/1998/namespace", "base");
      removeAttribute(attributes, "http://www.w3.org/2001/XMLSchema-instance", "schemaLocation");
   }

   private void removeAttribute(NamedNodeMap attributes, String namespaceURI, String localName) {
      if (attributes != null) {
         try {
            attributes.removeNamedItemNS(namespaceURI, localName);
         } catch (DOMException e) {
            //Ignore
         }
      }
   }

   private void filterNode(Node node) {
      if (node.hasChildNodes()) {
         Node child = node.getFirstChild();
         while (child != null) {
            if (child.getLocalName() == null && child.getNodeType() != Node.COMMENT_NODE) {
               Node toRemove = child;
               child = child.getNextSibling();
               node.removeChild(toRemove);
            } else {
               removeUnwantedAttributes(child);
               filterNode(child);
               child = child.getNextSibling();
            }
         }
      }
   }

   public static void main(String[] argv) throws Exception {
      XmlConfigFlattener flattener = new XmlConfigFlattener();
      Path outputDir = Paths.get(System.getProperty("user.dir"));
      Getopt opts = new Getopt("xml-flattener", argv, "vo:");
      for (int opt = opts.getopt(); opt > -1; opt = opts.getopt()) {
         switch (opt) {
            case 'v':
               flattener.setVerbose(true);
               break;
            case 'o':
               outputDir = Paths.get(opts.getOptarg());
               break;
         }
      }
      Files.createDirectories(outputDir);
      for (int i = opts.getOptind(); i < argv.length; i++) {
         flattener.flatten(Paths.get(argv[i]), outputDir);
      }
   }

   public static void prettyXml(Document document, Writer writer) throws TransformerException {
      // Setup pretty print options
      TransformerFactory transformerFactory = TransformerFactory.newInstance();
      transformerFactory.setAttribute("indent-number", 4);
      Transformer transformer = transformerFactory.newTransformer();
      transformer.setOutputProperty(OutputKeys.ENCODING, "UTF-8");
      transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
      transformer.setOutputProperty(OutputKeys.INDENT, "yes");
      transformer.transform(new DOMSource(document), new StreamResult(writer));
   }
}

package org.infinispan.tools.xsd;

import gnu.getopt.Getopt;

import java.io.File;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.Source;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.URIResolver;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;

import org.w3c.dom.Document;

public class XSDoc {
   private final Map<String, Document> xmls = new HashMap<String, Document>();
   private final Transformer xslt;
   private final DocumentBuilder docBuilder;

   XSDoc() throws Exception {
      TransformerFactory factory = TransformerFactory.newInstance();
      factory.setURIResolver(new URIResolver() {
         @Override
         public Source resolve(String href, String base) throws TransformerException {
            Document doc = xmls.get(getBaseFileName(href));
            if (doc != null) {
               return new DOMSource(doc);
            } else {
               return null;
            }
         }
      });
      ClassLoader cl = XSDoc.class.getClassLoader();
      InputStream xsl = cl.getResourceAsStream("xsd/xsdoc.xslt");
      xslt = factory.newTransformer(new StreamSource(xsl));
      DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
      dbf.setNamespaceAware(true);
      docBuilder = dbf.newDocumentBuilder();
   }

   void transform(String fileName, File outputDir) throws Exception {
      Document doc = docBuilder.parse(new File(fileName));
      String name = getBaseFileName(fileName);
      xmls.put(name, doc);
      xslt.transform(new DOMSource(doc), new StreamResult(new File(outputDir, name + ".html")));
   }

   public static String getBaseFileName(String absoluteFileName) {
      int slash = absoluteFileName.lastIndexOf('/');
      int dot = absoluteFileName.lastIndexOf('.');
      return absoluteFileName.substring(slash + 1, dot);
   }

   public static void main(String argv[]) throws Exception {
      XSDoc xsDoc = new XSDoc();
      String outputDir = System.getProperty("user.dir");
      Getopt opts = new Getopt("xsdoc", argv, "o:");
      for (int opt = opts.getopt(); opt > -1; opt = opts.getopt()) {
         switch (opt) {
         case 'o':
            outputDir = opts.getOptarg();
            break;
         }
      }
      File outDir = new File(outputDir);
      outDir.mkdirs();
      for (int i = opts.getOptind(); i < argv.length; i++) {
         xsDoc.transform(argv[i], outDir);
      }
   }
}

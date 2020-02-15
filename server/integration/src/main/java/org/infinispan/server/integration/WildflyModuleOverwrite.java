package org.infinispan.server.integration;

import java.io.File;
import java.io.IOException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.w3c.dom.Document;
import org.xml.sax.SAXException;

/**
 * We need to make sure that the current HotRod shipped with the new Infinispan version will works
 */
public class WildflyModuleOverwrite {

   static void change(File xmlModule, String jarName) {
      try {
         // read
         DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
         DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
         Document doc = dBuilder.parse(xmlModule);

         // change
         doc.getElementsByTagName("resource-root").item(0).getAttributes().getNamedItem("path").setNodeValue(jarName);

         // write
         TransformerFactory transformerFactory = TransformerFactory.newInstance();
         Transformer transformer = transformerFactory.newTransformer();
         transformer.setOutputProperty(OutputKeys.INDENT, "yes");
         transformer.transform(new DOMSource(doc), new StreamResult(xmlModule));
      } catch (ParserConfigurationException | IOException | SAXException | TransformerException e) {
         throw new IllegalStateException("Cannot parse the module.xml", e);
      }
   }
}

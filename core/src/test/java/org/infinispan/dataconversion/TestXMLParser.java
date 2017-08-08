package org.infinispan.dataconversion;

import static javax.xml.stream.XMLStreamConstants.CHARACTERS;
import static javax.xml.stream.XMLStreamConstants.START_ELEMENT;

import java.io.StringReader;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.XMLStreamWriter;

import org.xml.sax.SAXException;

class TestXMLParser {

   public Map<String, String> parse(String input) throws ParserConfigurationException, SAXException, XMLStreamException {
      XMLInputFactory factory = XMLInputFactory.newInstance();
      XMLStreamReader reader = factory.createXMLStreamReader(new StringReader(input));
      Map<String, String> pairs = new HashMap<>();
      String currentElement = null, currentValue = null;
      while (reader.hasNext()) {
         int event = reader.next();
         switch (event) {
            case START_ELEMENT:
               currentElement = reader.getLocalName();
               break;
            case CHARACTERS:
               currentValue = reader.getText();
               break;
            case XMLStreamConstants.END_ELEMENT:
               if (currentElement != null) {
                  pairs.put(currentElement, currentValue);
               }
               currentElement = null;
               currentValue = null;
               break;
         }

      }
      return pairs;
   }

   String serialize(Map<String, String> pairs) throws XMLStreamException {
      if (pairs.isEmpty()) return "";
      XMLOutputFactory factory = XMLOutputFactory.newInstance();

      StringWriter writer = new StringWriter();
      XMLStreamWriter streamWriter = factory.createXMLStreamWriter(writer);

      streamWriter.writeStartElement("root");
      for (Map.Entry<String, String> entry : pairs.entrySet()) {
         streamWriter.writeStartElement(entry.getKey());
         streamWriter.writeCharacters(entry.getValue());
         streamWriter.writeEndElement();
      }
      streamWriter.writeEndElement();
      return writer.toString();
   }

}

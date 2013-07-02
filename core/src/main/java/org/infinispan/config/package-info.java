/**
 * Cache configuration beans and parsers.
 * Need @XmlSchema annotation in order to classify all JAXB created schema elements in appropriate XML namespace
 */
@XmlSchema(namespace = ISPN_NS, elementFormDefault = XmlNsForm.QUALIFIED, attributeFormDefault = XmlNsForm.UNQUALIFIED, 
         xmlns = {
         @javax.xml.bind.annotation.XmlNs(prefix = "tns", namespaceURI = ISPN_NS),
         @javax.xml.bind.annotation.XmlNs(prefix = "xs", namespaceURI = "http://www.w3.org/2001/XMLSchema") })
package org.infinispan.config;

import javax.xml.bind.annotation.XmlNsForm;
import javax.xml.bind.annotation.*;
import static org.infinispan.config.parsing.NamespaceFilter.ISPN_NS;
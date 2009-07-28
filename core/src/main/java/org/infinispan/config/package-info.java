/**
 * Cache configuration beans and parsers.
 * Need @XmlSchema annotation in order to classify all JAXB created schema elements in appropriate XML namespace
 */
@XmlSchema(namespace = "urn:infinispan:config:4.0", elementFormDefault = XmlNsForm.QUALIFIED, attributeFormDefault = XmlNsForm.UNQUALIFIED, 
         xmlns = {
         @javax.xml.bind.annotation.XmlNs(prefix = "tns", namespaceURI = "urn:infinispan:config:4.0"),
         @javax.xml.bind.annotation.XmlNs(prefix = "xs", namespaceURI = "http://www.w3.org/2001/XMLSchema") })
package org.infinispan.config;

import javax.xml.bind.annotation.XmlNsForm;
import javax.xml.bind.annotation.*;
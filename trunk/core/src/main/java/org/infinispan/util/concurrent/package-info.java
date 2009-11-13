/**
 * Needed for IsolationLevel
 */
@XmlSchema(namespace = "urn:infinispan:config:4.0", elementFormDefault = XmlNsForm.QUALIFIED, attributeFormDefault = XmlNsForm.UNQUALIFIED, 
         xmlns = {
         @javax.xml.bind.annotation.XmlNs(prefix = "tns", namespaceURI = "urn:infinispan:config:4.0"),
         @javax.xml.bind.annotation.XmlNs(prefix = "xs", namespaceURI = "http://www.w3.org/2001/XMLSchema") })
package org.infinispan.util.concurrent;

import javax.xml.bind.annotation.XmlNsForm;
import javax.xml.bind.annotation.*;
/**
 * Thread-safe containers and other concurrency-related utilities, designed to supplement JDK concurrency utilities
 * and containers.
 */

// this annotation is needed since the IsolationLevel enumeration is in this package.
@XmlSchema(
      namespace = "urn:infinispan:config:4.0",
      elementFormDefault = QUALIFIED,
      attributeFormDefault = UNQUALIFIED,
      xmlns = {
         @XmlNs(prefix = "tns", namespaceURI = "urn:infinispan:config:4.0"),
         @XmlNs(prefix = "xs", namespaceURI = "http://www.w3.org/2001/XMLSchema") }
)
package org.infinispan.util.concurrent;

import javax.xml.bind.annotation.XmlNs;
import javax.xml.bind.annotation.XmlNsForm;
import javax.xml.bind.annotation.XmlSchema;

import static javax.xml.bind.annotation.XmlNsForm.QUALIFIED;
import static javax.xml.bind.annotation.XmlNsForm.UNQUALIFIED;

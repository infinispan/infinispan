package org.infinispan.transcoder.xml;

import org.kohsuke.MetaInfServices;

import reactor.blockhound.BlockHound;
import reactor.blockhound.integration.BlockHoundIntegration;

@SuppressWarnings("unused")
@MetaInfServices
public class XmlTranscoderBlockHoundIntegration implements BlockHoundIntegration {
   @Override
   public void applyTo(BlockHound.Builder builder) {
      // XStream uses reflection that can incur in illegal access being logged to disk due to modules isolation
      builder.allowBlockingCallsInside("com.thoughtworks.xstream.converters.reflection.SerializableConverter", "isSerializable");
   }
}

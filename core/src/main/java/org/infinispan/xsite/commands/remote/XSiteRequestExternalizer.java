package org.infinispan.xsite.commands.remote;

import org.infinispan.commons.marshall.AdvancedExternalizer;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Set;

import static org.infinispan.commons.marshall.Ids.XSITE_COMMANDS_EXTERNALIZER;

/**
 * Externalizer for {@link XSiteRequest} and its implementations.
 *
 * @since 15.0
 */
@SuppressWarnings({"rawtypes", "deprecation"})
public class XSiteRequestExternalizer implements AdvancedExternalizer<XSiteRequest> {

   public static final AdvancedExternalizer<XSiteRequest> INSTANCE = new XSiteRequestExternalizer();

   private XSiteRequestExternalizer() {
   }

   @Override
   public Set<Class<? extends XSiteRequest>> getTypeClasses() {
      return Ids.getTypeClasses();
   }

   @Override
   public Integer getId() {
      return XSITE_COMMANDS_EXTERNALIZER;
   }

   @Override
   public void writeObject(ObjectOutput output, XSiteRequest object) throws IOException {
      output.writeByte(object.getCommandId());
      object.writeTo(output);
   }

   @Override
   public XSiteRequest<?> readObject(ObjectInput input) throws IOException, ClassNotFoundException {
      return Ids.fromId(input.readByte()).readFrom(input);
   }
}

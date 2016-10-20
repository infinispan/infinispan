package org.infinispan.marshall.exts;

import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.util.Util;
import org.infinispan.marshall.core.Ids;
import org.infinispan.notifications.cachelistener.event.Event;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Set;

public final class CacheEventTypeExternalizer extends AbstractExternalizer<Event.Type> {

   @Override
   public Set<Class<? extends Event.Type>> getTypeClasses() {
      return Util.asSet(Event.Type.class);
   }

   @Override
   public Integer getId() {
      return Ids.CACHE_EVENT_TYPE;
   }

   @Override
   public void writeObject(ObjectOutput output, Event.Type object) throws IOException {
      output.writeByte(object.ordinal());
   }

   @Override
   public Event.Type readObject(ObjectInput input) throws IOException, ClassNotFoundException {
      return Event.Type.values()[input.readUnsignedByte()];
   }

}

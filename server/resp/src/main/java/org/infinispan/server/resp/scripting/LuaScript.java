package org.infinispan.server.resp.scripting;

import org.infinispan.scripting.impl.ScriptMetadata;

public record LuaScript(String code, ScriptMetadata metadata) {
   public String sha() {
      return metadata.properties().get("sha");
   }

   public long flags() {
      return Long.parseLong(metadata.properties().get("flags"));
   }
}

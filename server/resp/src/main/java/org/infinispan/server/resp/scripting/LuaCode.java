package org.infinispan.server.resp.scripting;

import java.util.Map;

import org.infinispan.scripting.impl.ScriptMetadata;
import org.infinispan.scripting.impl.ScriptWithMetadata;

public record LuaCode(String name, String code, String sha, long flags) {

   static LuaCode fromScript(ScriptWithMetadata script) {
      return fromScript(script.code(), script.metadata());
   }

   public static LuaCode fromScript(String script, ScriptMetadata metadata) {
      Map<String, String> properties = metadata.properties();
      return new LuaCode(properties.get("name"), script, properties.get("sha"), Long.parseLong(properties.get("flags")));
   }
}

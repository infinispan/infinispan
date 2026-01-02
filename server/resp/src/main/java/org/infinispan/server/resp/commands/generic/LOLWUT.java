package org.infinispan.server.resp.commands.generic;

import static org.infinispan.commons.util.Util.read;
import static org.infinispan.server.resp.RespUtil.ascii;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.infinispan.commons.util.Util;
import org.infinispan.commons.util.Version;
import org.infinispan.server.resp.AclCategory;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.Resp3Command;

import io.netty.channel.ChannelHandlerContext;

/**
 * LOLWUT
 *
 * @see <a href="https://redis.io/commands/lolwut/">LOLWUT</a>
 * @since 16.0
 */
public class LOLWUT extends RespCommand implements Resp3Command {
   public LOLWUT() {
      super(-1, 0, 0, 0, AclCategory.READ.mask() | AclCategory.FAST.mask());
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler,
                                                      ChannelHandlerContext ctx,
                                                      List<byte[]> arguments) {
      String version = Version.getMajorMinor();
      if (arguments.size() > 1 && "VERSION".equalsIgnoreCase(ascii(arguments.get(0)))) {
         version = ascii(arguments.get(1));
         Matcher matcher = Pattern.compile("([0-9]+)(\\.[0-9]+)?").matcher(version);
         if (matcher.matches()) {
            version = matcher.group(1) + (matcher.group(2) != null ? matcher.group(2) : ".0");
         } else {
            version = Version.getMajorMinor();
         }
      }
      StringBuilder s = new StringBuilder();
      InputStream is = LOLWUT.class.getResourceAsStream("/lolwut/" + version + ".txt");
      if (is == null) {
         is = LOLWUT.class.getResourceAsStream("/lolwut/infinispan.txt");
      }
      try {
         s.append(read(is));
         s.append('\n');
      } catch (IOException ignore) {
      }
      Util.close(is);

      s.append(Version.getBrandName());
      s.append(" ver. ");
      s.append(Version.getBrandVersion());
      s.append('\n');
      handler.writer().string(s);
      return handler.myStage();
   }
}

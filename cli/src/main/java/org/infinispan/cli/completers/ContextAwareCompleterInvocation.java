package org.infinispan.cli.completers;

import java.util.Collection;
import java.util.List;

import org.aesh.command.Command;
import org.aesh.command.completer.CompleterInvocation;
import org.aesh.readline.AeshContext;
import org.aesh.readline.terminal.formatting.TerminalString;
import org.infinispan.cli.Context;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class ContextAwareCompleterInvocation implements CompleterInvocation {
   private final CompleterInvocation delegate;
   protected final Context context;

   public ContextAwareCompleterInvocation(CompleterInvocation delegate, Context context) {
      this.delegate = delegate;
      this.context = context;
   }

   @Override
   public String getGivenCompleteValue() {
      return delegate.getGivenCompleteValue();
   }

   @Override
   public Command getCommand() {
      return delegate.getCommand();
   }

   @Override
   public List<TerminalString> getCompleterValues() {
      return delegate.getCompleterValues();
   }

   @Override
   public void setCompleterValues(Collection<String> completerValues) {
      delegate.setCompleterValues(completerValues);
   }

   @Override
   public void setCompleterValuesTerminalString(List<TerminalString> completerValues) {
      delegate.setCompleterValuesTerminalString(completerValues);
   }

   @Override
   public void clearCompleterValues() {
      delegate.clearCompleterValues();
   }

   @Override
   public void addAllCompleterValues(Collection<String> completerValues) {
      delegate.addAllCompleterValues(completerValues);
   }

   @Override
   public void addCompleterValue(String value) {
      delegate.addCompleterValue(value);
   }

   @Override
   public void addCompleterValueTerminalString(TerminalString value) {
      delegate.addCompleterValueTerminalString(value);
   }

   @Override
   public boolean isAppendSpace() {
      return delegate.isAppendSpace();
   }

   @Override
   public void setAppendSpace(boolean appendSpace) {
      delegate.setAppendSpace(appendSpace);
   }

   @Override
   public void setIgnoreOffset(boolean ignoreOffset) {
      delegate.setIgnoreOffset(ignoreOffset);
   }

   @Override
   public boolean doIgnoreOffset() {
      return delegate.doIgnoreOffset();
   }

   @Override
   public void setOffset(int offset) {
      delegate.setOffset(offset);
   }

   @Override
   public int getOffset() {
      return delegate.getOffset();
   }

   @Override
   public void setIgnoreStartsWith(boolean ignoreStartsWith) {
      delegate.setIgnoreStartsWith(ignoreStartsWith);
   }

   @Override
   public boolean isIgnoreStartsWith() {
      return delegate.isIgnoreStartsWith();
   }

   @Override
   public AeshContext getAeshContext() {
      return delegate.getAeshContext();
   }
}

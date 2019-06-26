public interface Vistor {
   Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable;

   Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable;

   Object visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable;

   Object visitClearCommand(InvocationContext ctx, ClearCommand command) throws Throwable;

   Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable;

   ... etc ...
}

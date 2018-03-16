/**
 * This package contains tooling for automatic generation of binary protocol parsers
 * implementing Netty's ByteToMessageDecoder. These parsers are expected to operate
 * without allocations (aside from allocations from the code specified in grammar).
 *
 * As the processing is quite general, ideally this code will end up in Netty project
 * itself. To demo its capabilities it is now used for decoding the Hot Rod protocol
 * and could be used for Memcached. Netty provides another means for HTTP parsing,
 * therefore REST is not a target.
 *
 * The syntax of grammar is intentionally close to ANTLRv4 syntax despite it's neither
 * a subset nor limited to that.
 *
 * <h2>Top-level statements</h2>
 *
 *
 * <code>namespace /identifier/;</code><br>
 * Defines an unique namespace for the elements.
 * This may be useful when the grammar includes another grammar (e.g. older version of protocol).
 * <p>
 * <code>class /fully qualified class name/ [extends /base class name/];</code><br>
 * Defines the package and name of the decoder class, possibly with a base class.
 * If the base class name is not set it defaults to {@link io.netty.handler.codec.ByteToMessageDecoder}.
 * <p>
 * <code>constants /class name/;</code><br>
 * Static final fields from this class can be referenced (without class name) in the grammar.
 * <p>
 * <code>intrinsics /class name/;</code><br>
 * This class is expected to contain static methods that will parse basic elements from the stream.
 * Each such method must have {@link io.netty.buffer.ByteBuf} as the first parameter
 * and any number (or none) of extra parameters.
 * Underscores on the end of method name are ignored; this allows the intrinsics to clash
 * with Java keywords, e.g. <code>byte</code>.
 * These intrinsics are referenced in the grammar using the method name (e.g. <code>string</code>),
 * if the method requires additional parameters these are passed in brackets (e.g. <code>array[16]</code>).
 * If the method advances buffer's {@link io.netty.buffer.ByteBuf#readerIndex() reader index} it's considered successful;
 * when it does not advance the reader index the parsing stops and the method will be invoked again when
 * more data arrives. Note that this limits non-reading intrinsics (e.g. arrays of length 0): these need
 * to be handled directly in the grammar using sentinels (see below).
 * <p>
 * Intrinsic methods should not have any side-effect.
 * <p>
 * <code>import /class name/;</code><br>
 * Adds this import to the generated parser.
 * <p>
 * <code>init { /custom code/ }</code><br>
 * This code will be added to the generated parser. This is the place to add constructor and extra fields
 * and methods.
 * <p>
 * <code>exceptionally { /custom code /}</code><br>
 * This code will be placed into a method called when an exception occurs during decoding process.
 * The exception is be provided as <code>Throwable t</code>.
 * <p>
 * <code>deadend() { /custom code/ }</code><br>
 * This code will be invoked when the parsing cannot continue, e.g. because there is no matching branch.
 * <p>
 * <code>[root] /rule name/ [returns /type/] [switch /reference/]: /branch1/ [| /branch 2/ | ...| /branch N/];</code><br>
 * Rules are the core part of parsing. There must be exactly one rule in each file with
 * the <code>root</code> modifier; this is where the parsing starts.
 * <p>
 * Each rule may return a type; in such case the parser contains a field that will hold the result.
 * The type can be either stated explicitly using <code>returns</code> or is inferred automatically.
 * Automatic inference requires all of the branches to be of the same type. Type for branch can be inferred when:
 * <li>there is only single reference to another rule in the branch
 * <li>the last statement in branch is an action (see below) that starts with <code>new /type/</code>
 * <li>the last statement in branch is an action that contains a number (possibly suffixed with L)
 * <li>the last statement in branch is an action that starts with <code>throw</code>
 * <p>
 * All but the last branch must start with a sentinel predicate:<pre><code>
 * myRule
 *    : { /predicate1/ }? /branch 1 elements/
 *    | { /predicate2/ }? /branch 2 elements/
 *    | /branch 3 elements/
 *    ;
 * </code></pre>
 * The first matching predicate triggers evaluation of the branch. Each predicate is a Java expression
 * that returns boolean. The predicates may (and should) reference already matched rules.
 * <p>
 * In order to generate a switch statement for a rule with many branches, the <code>switch</code> modifier
 * can be set on the rule. In that case the sentinels should not contain boolean predicates but values
 * that can be used as the switch cases.
 * <p>
 * The branch elements is a space-separated list of:
 * <li>references to another rules
 * <li>byte values provided as decimal (e.g. <code>42</code>) or hexadecimal (e.g <code>0xFF</code>).
 * <li>constants with value 0..255
 * <li>actions: <code>{ /custom code/ }</code>
 * <li>loops: <code>#/reference/ ( /elements/ )</code>
 * <p>
 * Loops require some more explanation here: the reference after <code>#</code> is not matched,
 * it is expected to be already matched before. At this point its value is evaluated and if it's not equal to zero
 * the referenced field is decremented and the following sequence of elements is matched. Here is an example:
 * <pre><code>
 * map returns Map&lt;String, String&gt;
 *    : numEntries { map = new HashMap&lt;&gt;(numEntries); } #numEntries ( key value { map.put(key, value); } )
 *    ;
 * </code></pre>
 * The loop can be used for unknown number of elements, too:
 * <pre><code>
 * list returns List&lt;String&gt;
 *    : { list = new ArrayList&lt;&gt;() } hasNext #hasNext ( value { list.add(value); } hasNext )
 *    ;
 * </code></pre>
 * <p>
 * The grammar must be non-recursive; rules can reference each other as long as this does not cause a cycle.
 * <p>
 * Once the root rule is fully matched, all fields are zeroed and the parsing starts with root rule anew.
 * The root rule is not expected to return anything the rightmost element in the parsing tree should be an action
 * that will fire a callback (defined in base class or in the <code>init</code> code).
 */
package org.infinispan.ppg.generator;

$(document).ready(function() {
    var prefix = "/docs/"
    var path = document.location.pathname;
    var version = path.substring(prefix.length, path.indexOf("/", prefix.length));
    $('#toctitle').before('<select id="dchooser"></select>');
    $('#dchooser').append('<option>Other guides</option>');
    $('#dchooser').append('<option value="/titles/cli/cli.html">Using the Infinispan Command Line Interface</option>');
    $('#dchooser').append('<option value="/titles/configuring/configuring.html">Configuring Infinispan caches</option>');
    $('#dchooser').append('<option value="/titles/contributing/contributing.html">Contributor&quot;s guide</option>');
    $('#dchooser').append('<option value="/titles/developing/developing.html">Infinispan Developer Guide</option>');
    $('#dchooser').append('<option value="/titles/embedding/embedding.html">Embedding Infinispan caches in Java applications</option>');
    $('#dchooser').append('<option value="/titles/encoding/encoding.html">Encoding Infinispan caches and marshalling data</option>');
    $('#dchooser').append('<option value="/titles/getting_started/getting_started.html">Getting started with Infinispan</option>');
    $('#dchooser').append('<option value="/titles/hibernate/hibernate.html">Hibernate second-level caching (2LC)</option>');
    $('#dchooser').append('<option value="/titles/hotrod_java/hotrod_java.html">Using Hot Rod Java clients</option>');
    $('#dchooser').append('<option value="/titles/hotrod_protocol/hotrod_protocol.html">Hot Rod protocol reference</option>');
    $('#dchooser').append('<option value="/titles/memcached/memcached.html">Using the Memcached protocol endpoint with Infinispan</option>');
    $('#dchooser').append('<option value="/titles/query/query.html">Querying Infinispan caches</option>');
    $('#dchooser').append('<option value="/titles/resp/resp-endpoint.html">Using the RESP protocol endpoint with Infinispan</option>');
    $('#dchooser').append('<option value="/titles/rest/rest.html">Using the Infinispan REST API</option>');
    $('#dchooser').append('<option value="/titles/security/security.html">Infinispan Security Guide</option>');
    $('#dchooser').append('<option value="/titles/server/server.html">Guide to Infinispan Server</option>');
    $('#dchooser').append('<option value="/titles/spring_boot/starter.html">Infinispan Spring Boot Starter</option>');
    $('#dchooser').append('<option value="/titles/spring/spring.html">Spring Cache and Spring Sessions with Infinispan</option>');
    $('#dchooser').append('<option value="/titles/tuning/tuning.html">Infinispan performance considerations and tuning guidelines</option>');
    $('#dchooser').append('<option value="/titles/upgrading/upgrading.html">Upgrading Infinispan deployments</option>');
    $('#dchooser').append('<option value="/titles/xsite/xsite.html">Infinispan cross-site replication</option>');
    $('#dchooser').change(function(e) {
       if (this.value !== '') {
          window.location.href = path.substring(0, path.indexOf('/titles/')) + this.value;
       }
    });
    $('#dchooser').after('<hr/>');
    $.ajax({type: 'GET', dataType: 'xml', url: '/docs/versions.xml',
            success: function(xml) {
                $('#toctitle').before('<select id="vchooser"></select>');
                $('#vchooser').append('<option>Choose version</option>');
                $(xml).find('version').each(function() {
                    var name = $(this).attr("name");
                    var selected = name.indexOf(version) == 0 ? "selected" : "";
                    $('#vchooser').append('<option value="' + $(this).attr("path") + '" ' + selected + '>' + name + '</option>');
                });
                $('#vchooser').change(function(e) {
                    if (this.value !== '')
                        window.location.href = path.replace(version, this.value);
                });
                $('#vchooser').after('<hr/>');
            }
    });

    $('ul.sectlevel1').wrap('<div id="toctree"></div>');

    $('#toctree').jstree({
        "core" : {
        "themes" : {"variant" : "small", "icons" : false}
    },
    "plugins" : [ "search", "state", "wholerow" ] })
          .on("activate_node.jstree", function (e, data) { location.href = data.node.a_attr.href; });
    $('#toctree').before('<input placeholder="&#xf002; Search" id="tocsearch" type="text">');
    var searchTimeout = false;
    $('#tocsearch').keyup(function () {
        if(searchTimeout) { clearTimeout(searchTimeout); }
        searchTimeout = setTimeout(function () {
            var v = $('#tocsearch').val();
            $('#toctree').jstree(true).search(v);
        }, 250);
    });
    $('#tocsearch').after('<a href="#" id="toctreeexpand" title="Expand"><i class="fa fa-plus-square" aria-hidden="true"></i></a><a href="#" id="toctreecollapse" title="Collapse"><i class="fa fa-minus-square" aria-hidden="true"></i></a>');
    $('#toctreeexpand').click(function() { $('#toctree').jstree('open_all'); });
    $('#toctreecollapse').click(function() { $('#toctree').jstree('close_all'); });
});

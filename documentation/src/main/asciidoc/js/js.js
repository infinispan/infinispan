$(document).ready(function() {
    var prefix = "/docs/"
    var path = document.location.pathname;
    var version = path.substring(prefix.length, path.indexOf("/", prefix.length));
    $('#toctitle').before('<select id="dchooser"></select>');
    $('#dchooser').append('<option>Documentation index</option>');
    $('#dchooser').append('<option value="/titles/getting_started/getting_started.html">Getting started</option>');
    $('#dchooser').append('<option value="/titles/tuning/tuning.html">Deployment planning and tuning</option>');
    $('#dchooser').append('<option value="/titles/configuring/configuring.html">Configuring caches</option>');
    $('#dchooser').append('<option value="/titles/encoding/encoding.html">Encoding and marshalling</option>');
    $('#dchooser').append('<option value="/titles/query/query.html">Querying caches</option>');
    $('#dchooser').append('<option value="/titles/xsite/xsite.html">Cross-site replication</option>');
    $('#dchooser').append('<option value="/titles/embedding/embedding.html">Embedding Infinispan caches</option>');
    $('#dchooser').append('<option value="/titles/server/server.html">Guide to Infinispan Server</option>');
    $('#dchooser').append('<option value="/titles/cli/cli.html">Command Line Interface (CLI)</option>');
    $('#dchooser').append('<option value="/titles/rest/rest.html">REST API</option>');
    $('#dchooser').append('<option value="/titles/hotrod_java/hotrod_java.html">Hot Rod Java clients</option>');
    $('#dchooser').append('<option value="/titles/hotrod_protocol/hotrod_protocol.html">Hot Rod protocol reference</option>');
    $('#dchooser').append('<option value="/titles/memcached/memcached.html">Memcached protocol endpoint</option>');
    $('#dchooser').append('<option value="/titles/resp/resp-endpoint.html">RESP protocol endpoint</option>');
    $('#dchooser').append('<option value="/titles/security/security.html">Security guide</option>');
    $('#dchooser').append('<option value="/titles/hibernate/hibernate.html">Hibernate second-level caching (2LC)</option>');
    $('#dchooser').append('<option value="/titles/spring_boot/starter.html">Spring Boot Starter</option>');
    $('#dchooser').append('<option value="/titles/spring/spring.html">Spring Cache and Spring Sessions</option>');
    $('#dchooser').append('<option value="/titles/upgrading/upgrading.html">Upgrading deployments</option>');
    $('#dchooser').append('<option value="/titles/contributing/contributing.html">Contributor&rsquo;s guide</option>');
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

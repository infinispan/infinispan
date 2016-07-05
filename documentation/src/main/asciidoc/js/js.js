$(document).ready(function() {
    var version = "{infinispanversion}.x";
    $.ajax({type: 'GET', dataType: 'xml', url: '../../versions.xml',
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
                        window.location.href = '../../' + this.value + '/user_guide/user_guide.html';
                });
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


function _ajax_request(url, data, method, callback) {
    return jQuery.ajax({
        url: url,
        type: method,
        data: data,
        success: callback
    });
}

function put() {

    var data = $('#submit-data').val()

    _ajax_request('/api/v1/documents', data, 'PUT', function(response) {
        console.log(response)
        location.reload()
    })

}


function post() {

    var data = $('#submit-data').val()

    _ajax_request('/api/v1/documents', data, 'POST', function(response) {
        console.log(response)
        location.reload()
    })

}


function compact() {

    _ajax_request('/api/v1/documents/compact', null, 'GET', function(response) {
        console.log(response)
        new PNotify({
            title: 'Compaction Succesfull',
            text: response
        });
    })

}


$('#submit-post').click(post);

$('#submit-put').click(put);
$('#submit-compact').click(compact);
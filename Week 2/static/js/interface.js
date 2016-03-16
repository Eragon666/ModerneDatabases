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

    _ajax_request('/api/v1/document', data, 'PUT', function(response) {
        console.log(response)
        location.reload()
    })

}


function post() {

    var data = $('#submit-data').val()

    _ajax_request('/api/v1/document', data, 'POST', function(response) {
        console.log(response)
        location.reload()
    })

}


$('#submit-post').click(post);

$('#submit-put').click(put);
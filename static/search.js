$(function() {
    $('#btnSignUp').click(function() {
 
        $.ajax({
            url: '/search',
            data: $('form').serialize(),
            type: 'GET',
            success: function(response) {
                console.log(response);
            },
            error: function(error) {
                console.log(error);
            }
        });
    });
});
$(document).ready(function() {

    /*$("#query").autocomplete({
            source: function(request, response) {
                $.ajax({
                    url: "AutoComplete",
                    type: "POST",
                    data: {chars: $("#query").val()},
    
                    dataType: "json",
    
                    success: function(data) {
    	    response(data);
                    }
               });              
            }   
    });*/

    $("#query").keydown(function(event) {
        if (event.keyCode == 13) {
            //doSearch();
        	$("#searchButton").click();
        }
    });

    $(window).resize(function() {
        removeGraph();
        drawGraph();
    });
});

function doSearch() {
    if ($("#query").val() != "") {
        $.ajax({
            url: "SearchVocab",
            data: {
                "concept": $("#query").val().toLowerCase()
            },
            success: function completeHandler(response) {
                if (response != null) {
                    $("#searchContainer").css("margin-top", "30px");
                    $("#searchResultContainer").show();
                    $("#searchContainer h2.title").css("font-size", "24px");

                    graph = response.graph;
                    $("#searchResultsGroup").append($("#searchGroup"));

                    $("#searchResults").show();
                    removeGraph();
                    drawGraph();
                }
            }
        });
    }
}
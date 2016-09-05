$(document).ready(function(){
    
     $("#query").autocomplete({
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
    });
    
        $("#query").keyup(function(event){
    if(event.keyCode == 13){
    	$("#searchButton").click();
    }
    });
    
    $("#searchButton").click(function() {
      if($("#query").val()!="")
    {
    $.ajax({
    url : "SearchVocab",
    data : {
    			"concept" : $("#query").val().toLowerCase()
    		},
    success : function completeHandler(response) {
        if(response!=null)
    	{
    	console.log(response);
    	    graph = response.graph;
    		$("#searchResultsGroup").append($("#searchGroup"));
    		
    		$("#searchResults").show();
    		removeGraph();
    		drawGraph();
    	
    		//writeFilterList(response.filters.filters);
    	}					
    }
    });		
    
    }
    });
    });
	
	$(window).resize(function() {
    	removeGraph();
    	drawGraph();
    });
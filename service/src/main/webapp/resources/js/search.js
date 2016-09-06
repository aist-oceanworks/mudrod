$(document).ready(function(){
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
    
		var query = getURLParameter('query');
		if(query==null)
		{			
			$("#searchResults").hide();
		}else{
			$("#searchResults").hide();
			$("#NotFound").hide();
			$("#query").val(query);
			search(query);
		}

		$("#query").keyup(function(event){
			if(event.keyCode == 13){
				$("#searchButton").click();
			}
		});		
		
		$("#searchButton").click(function() {				
			setGetParameter("query", $("#query").val());
	   });
    
    });
    
    function search(query){
    if($("#query").val()!="")
    			{							
    			$("#searchBox").append($("#searchGroup"));
    			$("#searchjumbo").hide();
    			$("#note").hide();
    			$("#searchResults").show();
    			$("#searchLoading").show();
    			
            	$("#searchContainer").css("margin-top", "30px");
            	$("#searchResultContainer").show();
            	$("#searchContainer h2.title").css("font-size", "24px");
    			$.ajax({
    				url : "SearchMetadata",
    				data : {
    							"query" : $("#query").val()
    					   },
    				success : function completeHandler(response) {
    					if(response!=null)
    					{
    						$("#searchLoading").hide();
    						
    						var searchResults = response.PDResults;
    						if(searchResults.length==0)
    						{
    						$("#NotFound").show();
    						}else{
    						createResultTable();
    						$('#ResultsTable').bootstrapTable('load', searchResults);
    						}
    					}					
    				}
    			});		
    	
    		   }
    }

    function FileNameFormatter(value) {
        var url = "http://podaac.jpl.nasa.gov/ws/metadata/dataset?format=gcmd&shortName="+encodeURIComponent(value);
           //url = encodeURIComponent(url);		
        url = "./dataset.html?shortname=" + value;
    	return '<a href=' + url + ' target="_blank">' + value + '</a>'; 
       }

    function createResultTable() {
    	var layout = {
    		cache : false,
    		pagination : true,
    		pageSize : 10,
    		striped: true,
    		//pageList : [ 10, 25, 50, 100, 200 ],
    		//sortName : "Time",
    		//sortOrder : "asc",
    		cardView: true,
			showHeader: false,
    
    		columns : [ {
    			'title' : 'Short Name',
    			'field' : 'Short Name',
    			'formatter' : FileNameFormatter,
    			sortable : true
    		}, {
    			'title' : 'Long Name',
    			'field' : 'Long Name',
    		},
    		{
    			'title' : 'Topic',
    			'field' : 'Topic',
				'formatter' : TopicFormatter,    
    		}, 
    		{
    			'title' : 'Release Date',
    			'field' : 'Release Date',  
    		},{
    			'title' : 'Abstract',
    			'field' : 'Abstract',  
    		} ]
    
    	};
    
    	$('#ResultsTable').bootstrapTable(layout);
    }
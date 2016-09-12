var g_currentQuery, g_currentSearchOption;
var shortname = getURLParameter('shortname');

  $(document).ready(function(){
		var query = getURLParameter('query');
		g_currentSearchOption = getURLParameter('searchOption');
		if (query) {
			$("#query").val(query);
		}
		if(g_currentSearchOption) {
			$('#searchOption_' + g_currentSearchOption).prop('checked', true);
		}

		$("#query").keydown(function(event){
			if(event.keyCode == 13){
				$("#searchButton").click();
			}
		});		

		$("#searchButton").click(function() {
			redirect("search", "query", $("#query").val(), "searchOption", $("input[name='searchOption']:checked").val());
		});

		$("#ontologyUL").on("click", "li a", function(){
			redirect("search", "query", $(this).data("word"), "searchOption", $("input[name='searchOption']:checked").val());
		});

		g_currentQuery = encodeURI(query);

		$.ajax({
            url: "SearchVocab",
            data: {
                "concept": query,
				"operator": $("input[name='searchOption']:checked").val()
            },
            success: function completeHandler(response) {
                if (response != null) {
                    var ontologyResults = response.graph.ontology;
                    if(ontologyResults.length == 0) {
                    	
                    } else {
						//$("#ontologyResultCount").html(searchResults.length + ' Matching Collections');
                    	for(var i = 0; i < ontologyResults.length; i++){
                    		$("#ontologyUL").append("<li><a data-word='" + ontologyResults[i].word  + "' href='#'>" + ontologyResults[i].word + " (" + ontologyResults[i].weight + ")</a></li>");
                    	}                    	
                    }
                }
            }
        });
	 loadMetaData(shortname);
	 //loadRecomData(shortname);
	 loadHybirdRecomData(shortname);
  });
  
  function loadRecomData(shortname) {
	if (shortname != "") {
		$("#dataset").show();
		$.ajax({
			url : "RecomDatasets",
			data : {
				"shortname" : shortname
			},
			success : function completeHandler(response) {
				if (response != null) {
					var recomdata = response.recomdata;
					
					var linked = recomdata.linked;
					if (linked.length == 0) {
						$("#NotFound").show();
					} else {
						$.each(linked, function(i, item) {
					       var li = $("<li><a></a></li>");
					       $("#linkedul").append(li);
					       $("a",li).text(item.name  + "(" + item.weight + ")");
					       //$("a",li).text(item.name);
					       $("a",li).attr("href", "./dataset.html?shortname=" + item.name);
					    });
					}
					
					var related = recomdata.related;
					if (related.length == 0) {
						$("#NotFound").show();
					} else {
						$.each(related, function(i, item) {
					       var li = $("<li><a></a></li>");
					       $("#relatedul").append(li);
					       $("a",li).text(item.name + "(" + item.weight + ")");
					       //$("a",li).text(item.name);
					       $("a",li).attr("href", "./dataset.html?shortname=" + item.name);
					    });
					}
					
					loadHybirdRecomData(shortname);
				}
			}
		});
	}
  }
  
  function loadHybirdRecomData(shortname) {
		if (shortname != "") {
			$("#dataset").show();
			$.ajax({
				url : "HybirdRecomDatasets",
				data : {
					"shortname" : shortname,
					"operator": $("input[name='searchOption']:checked").val()
				},
				success : function completeHandler(response) {
					if (response != null) {
						var recomdata = response.recomdata;
						var linked = recomdata.linked;
						if (linked.length == 0) {
							$("#NotFound").show();
						} else {
							
							$.each(linked, function(i, item) {
								console.log("hh");
						       var li = $("<li><a></a></li>");
						       $("#hybirdul").append(li);
						       //$("a",li).text(item.name  + "(" + item.weight + ")");
						       $("a",li).text(item.name);
						       $("a",li).attr("href", "./dataset.html?query=" + g_currentQuery + "&searchOption=" + g_currentSearchOption + "&shortname=" + item.name);
						    });
						}
					}
				}
			});
		}
	}
    
  function loadMetaData(shortname) {
		if (shortname != "") {
			$("#dataset").show();
			$.ajax({
				url : "DatasetDetail",
				data : {
					"shortname" : shortname
				},
				success : function completeHandler(response) {
					if (response != null) {
						$("#searchLoading").hide();
						var searchResults = response.PDResults;
						
						console.log(searchResults);
						if (searchResults.length == 0) {
							$("#NotFound").show();
						} else {
							createResultTable();
							
							$("#shortName").html(searchResults[0]["Short Name"]);
							$('#ResultsTable').bootstrapTable('load', searchResults);
						}
					}
				}
			});
		}
	}
	
	function createResultTable() {
		var layout = {
			cache : false,
					cardView : true,
					columns : [ {
						'title' : 'Long Name',
						'field' : 'Long Name',
					}, {
						'title' : 'Topic',
						'field' : 'Topic',
					},  {
						'title' : 'Category',
						'field' : 'DatasetParameter-Category',
					} , {
						'title' : 'Variable',
						'field' : 'DatasetParameter-Variable',
					} , {
						'title' : 'Term',
						'field' : 'DatasetParameter-Term',
					} ,{
						'title' : 'Release Date',
						'field' : 'Release Date',

					}, {
						'title' : 'Abstract',
						'field' : 'Abstract',

					} , {
						'title' : 'Processing Level',
						'field' : 'Processing Level',

					} , {
						'title' : 'Doi',
						'field' : 'Dataset-Doi',

					} , {
						'title' : 'TemporalRepeat',
						'field' : 'Dataset-TemporalRepeat',

					} , {
						'title' : 'TemporalRepeatMax',
						'field' : 'Dataset-TemporalRepeatMax',

					} , {
						'title' : 'TemporalRepeatMin',
						'field' : 'Dataset-TemporalRepeatMin',

					} , {
						'title' : 'Sensor',
						'field' : 'DatasetSource-Sensor-ShortName',
					}, {
						'title' : 'Project',
						'field' : 'DatasetProject-Project-ShortName',
					} , {
						'title' : 'Format',
						'field' : 'DatasetPolicy-DataFormat',
					} , {
						'title' : 'DataLatency',
						'field' : 'DatasetPolicy-DataLatency',
					} 
					]
				};

				$('#ResultsTable').bootstrapTable(layout);
			}
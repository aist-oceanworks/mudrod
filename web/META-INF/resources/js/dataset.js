/*
 * Licensed under the Apache License, Version 2.0 (the "License"); you 
 * may not use this file except in compliance with the License. 
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
var g_currentQuery, g_currentSearchOption;
var shortname = getURLParameter('shortname');

$(document).ready(function () {
    var query = getURLParameter('query');
    g_currentSearchOption = getURLParameter('searchOption');
    if (query) {
        $("#query").val(query);
    }
    if (g_currentSearchOption) {
        $('#searchOption_' + g_currentSearchOption).prop('checked', true);
    }

    $("#query").keydown(function (event) {
        if (event.keyCode == 13) {
            $("#searchButton").click();
        }
    });

    $("#searchButton").click(function () {
        redirect("search", "query", $("#query").val(), "searchOption", $("input[name='searchOption']:checked").val());
    });

    $("#ontologyUL").on("click", "li a", function () {
        redirect("search", "query", $(this).data("word"), "searchOption", $("input[name='searchOption']:checked").val());
    });

    g_currentQuery = encodeURI(query);

    // invoke SearchVocabResource.java
    loadVocubulary(query);
    loadMetaData(shortname);
    loadHybirdRecomData(shortname);
});

function loadVocubulary(query) {
    $.ajax({
        url: "services/vocabulary/search",
        data: {
            "query": query
        },
        success: function completeHandler(response) {
            if (response != null && !jQuery.isEmptyObject(response)) {
                var ontologyResults = response.graph.ontology;
                if (ontologyResults.length == 0) {
                    $("#ontologyUL").append("<li>Did not find any results.</li>");
                } else {
                    for (var i = 0; i < ontologyResults.length; i++) {
                        $("#ontologyUL").append("<li><a data-word='" + ontologyResults[i].word + "' href='#'>" + ontologyResults[i].word + " (" + ontologyResults[i].weight + ")</a></li>");
                    }
                }
            }
        }
    });
}

function loadHybirdRecomData(shortname) {
    if (shortname != "") {
        $("#dataset").show();
        $.ajax({
            url: "services/hrecommendation/search",
            data: {
                "shortname": shortname
            },
            success: function completeHandler(response) {
                if (response != null && !jQuery.isEmptyObject(response)) {
                    var recomdata = response.HybridRecommendationData;
                    var linked = recomdata.linked;
                    if (linked.length == 0) {
                        $("#hybirdul").append("<li>Did not find any results.</li>");
                    } else {

                        $.each(linked, function (i, item) {
                            console.log("hh");
                            var li = $("<li><a></a></li>");
                            $("#hybirdul").append(li);
                            //$("a",li).text(item.name  + "(" + item.weight + ")");
                            $("a", li).text(item.name);
                            $("a", li).attr("href", "./dataset.html?query=" + g_currentQuery + "&searchOption=" + g_currentSearchOption + "&shortname=" + item.name);
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
            url: "services/datasetdetail/search",
            data: {
                "shortname": shortname
            },
            success: function completeHandler(response) {
                if (response != null && !jQuery.isEmptyObject(response)) {
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

function urlFormatter(value, row) {
	var urls = value.split(",");
	var html = "";
	for (i = 0; i < urls.length; i++) { 
	    html += '<a href=' + urls[i] + ' target="_blank">' + urls[i] + '</a>' + "<br>";
	}
	return html; 
}

function landingPageFormatter(value, row) {
	var html = "https://podaac.jpl.nasa.gov/dataset/";	
	html = '<a href=' + html + value + ' target="_blank">' + html + value + '</a>';	
	return html; 
}

function createResultTable() {
    var layout = {
        cache: false,
        cardView: true,
        columns: [{
            'title': 'Long Name',
            'field': 'Long Name',
        }, {
            'title': 'Doi',
            'field': 'Dataset-Doi',
        }, {
            'title': 'Topic',
            'field': 'Topic',
        }, {
            'title': 'Category',
            'field': 'DatasetParameter-Category',
        }, {
            'title': 'Variable',
            'field': 'DatasetParameter-Variable',
        }, {
            'title': 'Term',
            'field': 'DatasetParameter-Term',
        }, {
            'title': 'Version',
            'field': 'Version',
        }, {
            'title': 'Description',
            'field': 'Dataset-Description',
        }, {
            'title': 'Processing Level',
            'field': 'Processing Level',
        },{
            'title': 'Region',
            'field': 'Region',
        }, {
            'title': 'Coverage',
            'field': 'Coverage'
        }, {
            'title': 'Time Span',
            'field': 'Time Span',
        }, {
            'title': 'Spatial Resolution',
            'field': 'SpatiallResolution',
        }, {
            'title': 'Temporal Repeat',
            'field': 'TemporalResolution',
        }, {
            'title': 'Sensor',
            'field': 'DatasetSource-Sensor-ShortName',
        }, {
            'title': 'Project',
            'field': 'DatasetProject-Project-ShortName',
        }, {
            'title': 'Format',
            'field': 'DataFormat',
        }, {
            'title': 'Data Access',
            'field': 'DatasetLocationPolicy-BasePath',
            'formatter' : urlFormatter
        },
        {
            'title': 'Landing Page',
            'field': 'Short Name',
            'formatter' : landingPageFormatter
        }
        ]
    };

    $('#ResultsTable').bootstrapTable(layout);
}
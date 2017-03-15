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
$(document).ready(function () {

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

    $("#query").keydown(function (event) {
        if (event.keyCode == 13) {
            //doSearch();
            $("#searchButton").click();
        }
    });

    $(window).resize(function () {
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
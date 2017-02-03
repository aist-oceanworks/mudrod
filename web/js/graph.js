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
/*var graph =
{
  "nodes":[
    {"name":"ocean wind","group":1},
    {"name":"Greco","group":10},
    {"name":"Mackerel","group":10},
    {"name":"surface wind","group":10},
	{"name":"quikscat","group":10},
	{"name":"ccmp","group":10}

  ],
  "links":[
    {"source":1,"target":0,"value":1},
	{"source":2,"target":0,"value":1},
	{"source":3,"target":0,"value":1},
	{"source":4,"target":0,"value":1},
	{"source":5,"target":0,"value":1} 

  ]
};*/
var graph = null;


var nodeRadiusNormal = 5;
var nodeRadiusHover = 8;
var nodeFillHover = "#337ab7";
var nodeTextSizeNormal = "18px";
var nodeTextSizeHover = "30px";
var nodeTextFillNormal = "black";
var nodeTextFillHover = "#337ab7";
var WIDTH = 960, HEIGHT = 600;
var ZOOM, DRAG;
var ZOOM_MIN = 0.5, ZOOM_MAX = 2;

function writeFilterList(filterList){
	$('#filterBody').empty();
	var text = "";
	for(var i = 0 ; i<filterList.length; i ++)
	{
		//text = text + filterList[i].filter + "</br>";
		text = text + "<h4><span class='label label-default'>" + filterList[i].filter + "</span><h4>";
	}
	$("#filterBody").html(text);
}


function drawGraph(){
	//var width = 960,
	//    height = 500;

	WIDTH = $("#graphBody").width();
	HEIGHT = $("#graphBody").height();
	var color = d3.scale.category20();

	var force = d3.layout.force()
	.nodes(graph.nodes)
	.links(graph.links)
	.size([WIDTH, HEIGHT]);

	// set force graph measurements
	force.charge(scaledCharge(force.nodes().length, WIDTH, HEIGHT))
	.linkDistance(200)
	.on("tick", tick)
	.start();

	// set up zoom
	ZOOM = d3.behavior.zoom()
	.scaleExtent([ZOOM_MIN, ZOOM_MAX])
	.on("zoom", function() {

		// split x,y mouse pan events into two variables
		var eventX = d3.event.translate[0];
		var eventY = d3.event.translate[1];

		// scale min and max zoom to a value from 1 to 2
		var zoomScale = d3.scale.linear()
		.domain([ZOOM_MIN, ZOOM_MAX])
		.range([1,2]); 

		// define min and max x,y values based on current zoom
		var x_min = zoomScale(d3.event.scale) * WIDTH * -1; 
		var x_max = zoomScale(d3.event.scale) * WIDTH; 
		var y_min = zoomScale(d3.event.scale) * HEIGHT * -1; 
		var y_max = zoomScale(d3.event.scale) * HEIGHT;

		// restrict x,y values within bounds
		eventX = (eventX < x_min) ? x_min : eventX;
		eventX = (eventX > x_max) ? x_max : eventX;
		eventY = (eventY < y_min) ? y_min : eventY;
		eventY = (eventY > y_max) ? y_max: eventY;

		// manually set ZOOM's translate coordinates and update
		ZOOM.translate([eventX, eventY]);
		svg.attr("transform", "translate(" + ZOOM.translate() + ")scale(" + d3.event.scale + ")");
		//OR EQUIVALENT: svg.attr("transform", "translate(" + d3.event.translate + ")scale(" + d3.event.scale + ")");
	});

	//set up drag (prevent conflicts with force graph dragging)
	DRAG = d3.behavior.drag()
	.origin( function (d) { return d; })
	.on("dragstart", function(d) {
		d3.event.sourceEvent.stopPropagation(); // prevent entire svg from being dragged
		force.stop(); // temporarily stop force graph while dragging
	})
	.on("drag", function(d) {
		d.px += d3.event.dx;
		d.py += d3.event.dy;
		d.x += d3.event.dx;
		d.y += d3.event.dy; 
		tick();
	})
	.on("dragend", function (d) {
		d.fixed = true; //fix nodes once dragged
		if (d3.event.sourceEvent.which != 1) //unfix nodes if right-mouse click
			d.fixed = false;			
		tick();
		force.resume(); // resume force graph after dragging
	});

	var svg = d3.select("#graphBody").append("svg")
	.attr("width", WIDTH)
	.attr("height", HEIGHT)
	.attr("id", "d3-svg")
	.call(ZOOM)
	.on("dblclick.zoom", null) // disable tap-zooming (double-click expansion, etc.)
	.attr("preserveAspectRatio", "xMidYMid meet")
	.append("g");


	//add link
	var link = svg.selectAll(".link")
	.data(graph.links)
	.enter().append("line")
	.attr("class", "link")
	.style("stroke-width", function(d) { return Math.sqrt(d.value); });

	var linktext = svg.selectAll("g.linkTextDiv").data(graph.links);
	linktext.enter().append("g").attr("class", "linkTextDiv")
	.append("text")
	.attr("class", "linkText")
	.attr("dx", 1)
	.attr("dy", ".35em")
	.attr("text-anchor", "middle")
	.text(function(d) { return d.value })
	.style("fill", "black")
	.style("font-size", "12px");

	//add node class
	var node = svg.selectAll(".node")
	.data(graph.nodes)
	.enter().append("g")
	.attr("class", "node")
	.style("fill", function(d) { return color(d.group); })
	.on("mouseover", function(d) {
		this.style.cursor = 'pointer';
		d3.select(this).select("circle")
		.transition()
		.duration(200)
		.attr("r", nodeRadiusHover)
		.style("fill", nodeFillHover);
		d3.select(this).select("text")
		.transition()
		.duration(200)
		.style("font-size", nodeTextSizeHover)
		.style("fill", nodeTextFillHover);
	})
	.on("mouseout", function(d) {
		d3.select(this).select("circle")
		.transition()
		.duration(200)
		.attr("r", nodeRadiusNormal)
		.style("fill", function(d) { return color(d.group); })
		d3.select(this).select("text")
		.transition()
		.duration(200)
		.style("font-size", nodeTextSizeNormal)
		.style("fill", nodeTextFillNormal);
	})
	.on("dblclick", function (d) { 
		var text = d.name;
		$("#query").val(text);
		$("#searchButton").click();
	})
	.call(DRAG);

	// add circles to nodes
	node.append("circle")
	.attr("r", nodeRadiusNormal);

	// add text to nodes
	node.append("text")
	.attr("x", 12)
	.attr("dy", ".35em")
	.text(function (d) { return d.name; })
	.style("fill", "black")
	.style("font-size", nodeTextSizeNormal)
	.style("backround", "black");

	// determine positions of links (including text) and nodes on tick
	function tick() {
		link.attr("x1", function (d) { return d.source.x; })
		.attr("y1", function (d) { return d.source.y; })
		.attr("x2", function (d) { return d.target.x; })
		.attr("y2", function (d) { return d.target.y; });

		linktext.attr("transform", function (d) {
			avgX = (d.source.x + d.target.x)/2;
			avgY = (d.source.y + d.target.y)/2;

			return "translate(" + avgX + "," + avgY + ")";
		});

		node.attr("transform", function(d) { return "translate(" + d.x + "," + d.y + ")"; });
	}
}

//removes graph
function removeGraph() {
	d3.selectAll("#d3-svg").remove(); // remove svg element to reset the data
}

//rough (arbitrary) estimate to scale force graph charge vs. number of nodes
function scaledCharge(numNodes, width, height) {
	var k = Math.sqrt(numNodes/(width*height));
	if(numNodes < 50)
		return -5/k/2/(numNodes/10);
	else
		return -5/k/(Math.sqrt(numNodes/10)*2);
}

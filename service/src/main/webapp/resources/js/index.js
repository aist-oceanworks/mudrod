$(document).ready(function() {
		$("#query").keyup(function(event){
			if(event.keyCode == 13){
				$("#searchButton").click();
			}
		});		
		
		$("#searchButton").click(function() {				
			$("#searchForm").submit();
	   });
    });
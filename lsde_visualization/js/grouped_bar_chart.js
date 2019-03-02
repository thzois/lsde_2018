var years = [2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017];
var months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sept", "Oct", "Nov", "Dec"]
              
// Creates a bar chart to compare teh size of the graph between years 
function grouped_bar_chart(ctx, filename, title, xLabel){

	colors = ["#f38b4a", "#3e95cd"]

	cc_global = new Array();
	cc_avg = new Array();
	idx = 0;
  
	// Get results file 
	var req = new XMLHttpRequest();  
	req.open('GET', 'results/year/' + filename, false);   
	req.send();  
	if(req.status == 4 || req.status == 200){  
	  // Read data and split to lines
	  var data = (req.responseText).match(/[^\r\n]+/g);
	}else{
	  alert("Could not load file " + filename);
	}

  // We don't need the first line
  for(j = 1; j < data.length; j++){
      cc_global[idx] = Number(data[j].split(",")[1]);
      cc_avg[idx] = Number(data[j].split(",")[2]);
    	idx++;
  }

	// Create the chart
	var myChart = new Chart(ctx, {
                                responsive: true,
                                type: 'bar',
                                data: {
                                  labels: years,
                                  datasets: [
                                      { 
                                        data: cc_global,
                                        label: "Global",
                                        backgroundColor: "#f38b4a",
                                        borderColor: "#f38b4a"
                                      },
                                      {
                                        label: "Average",
                                        backgroundColor: "#0582CA",
                                        borderColor: "#0582CA",
                                        data: cc_avg
                                      }
                                  ]
                                },
                                options: {
                                  title: {
                                      display: true,
                                      text: title
                                  },
                                  legend: {
                                      position: 'top'
                                  },
                                  scales: {
                                    yAxes: [{
                                      ticks: {
                                          beginAtZero: true,
                                          min: 0,
                                          max: 1
                                      },
                                      scaleLabel: {
                                        display: true
                                      }
                                    }],
                                    xAxes: [{
                                      scaleLabel: {
                                        display: true,
                                        labelString: xLabel
                                      }
                                    }]
                                  } 
                                }
                            });
}

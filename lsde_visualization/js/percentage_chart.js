var years = [2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017];
var months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sept", "Oct", "Nov", "Dec"]
              
// Creates a bar chart to compare teh size of the graph between years 
function percentage_chart(ctx, filename, title, yLabel, xLabel){

  // Get results file 
  var req = new XMLHttpRequest();  
  req.open('GET', 'results/' + filename, false);   
  req.send();  
  if(req.status == 4 || req.status == 200){  
    // Read data and split to lines
    var data = (req.responseText).match(/[^\r\n]+/g);
  }else{
    alert("Could not load file " + filename);
  }

  var selNum_edges = [];
  var percentage_per_year = new Array();
  var idx = 0;

  // Construct the data for the chart
  // We don't need the first line (headers)
  for (i = 1; i < data.length; i++) {
    parts = data[i].split(",");
    total_edges = Number(parts[2]);
    selected_edges = Number(parts[3]);
    percentage_per_year[idx] = (selected_edges*100)/total_edges;
    selNum_edges[idx] = selected_edges;
    idx++;
  }
  
  // Handle ticks of ratio
  custom_ticks = {};
  custom_ticks["min"] = 0;
  custom_ticks["max"] = 100;
  custom_ticks["beginAtZero"] = true;

  // Create the chart
  var myChart = new Chart(ctx, {
                                responsive: true,
                                type: 'bar',
                                data: {
                                labels: years,
                                  datasets: [
                                      { 
                                        data: percentage_per_year,
                                        label: "Bars",
                                        backgroundColor: "#f38b4a",
                                        borderColor: "#f38b4a"
                                      },
                                      {
                                        type: "line",
                                        label: "Line",
                                        borderColor: "#0582CA",
                                        fill: true,
                                        borderWidth: 2,
                                        data: percentage_per_year,
                                        pointBackgroundColor: "#0582CA"
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
                                  tooltips: {
                                      callbacks: {
                                          label: function(tooltipItem, data) {
                                              new_label = "Selected edges: " + parseInt(selNum_edges[tooltipItem.index]);
                                              return new_label;
                                          }
                                      }
                                  },
                                  scales: {
                                    yAxes: [{
                                     ticks: custom_ticks,
                                      scaleLabel: {
                                        display: true,
                                        labelString: yLabel,
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

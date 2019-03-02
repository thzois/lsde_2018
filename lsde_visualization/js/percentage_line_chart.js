var years = [2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017];
var months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sept", "Oct", "Nov", "Dec"]
              
// Creates a line chart for month comparison between months 
function percentage_line_comparison_months(ctx, filename, title, yLabel, xLabel){

  // Get results file 
  var req = new XMLHttpRequest();  
  req.open('GET', 'results/'+filename, false);   
  req.send();  
  if(req.status == 4 || req.status == 200){  
    // Read data and split to lines
    var data = (req.responseText).match(/[^\r\n]+/g);
  }else{
    alert("Could not load file " + filename);
  }


  // Construct the JSON for each year 
  jsonData = {};
  var percentage_months = new Array();
  var percentage_per_year_month = new Array();

  var selected_months = new Array();
  var selected_per_year_month = new Array();

  var idx_year = 0;
  var idx = 0;

  colors = ["#f38b4a", "#0582CA", "#963484", "#E5CA1B", "#F8333C", "#3F7D20", "#7B7C7C", "#000000", "#932C2C"]

  for (i = 1; i < data.length; i++) {

    parts = data[i].split(",");
    year = String(parts[0]);
    total_edges = Number(parts[3]);
    selected_edges = Number(parts[4]);

    percentage_months[idx] = (selected_edges*100)/total_edges;
    selected_months[idx] = selected_edges;
    idx++;

    //Switch year - Construct JSON for chart
    if(i%12 == 0 || (year == "2017" && idx == 3)){

      jsonData["label"] = year;
      jsonData["data"] = percentage_months;
      jsonData["borderColor"] = colors[idx_year];
      jsonData["backgroundColor"] = colors[idx_year];
      jsonData["fill"] = false;
      jsonData["borderWidth"] = 2;
    

      // Hide every line except the first three
      if(year != "2009" && year != "2010" && year != "2011"){
        jsonData["hidden"] = true;
      }

      // Change line for the year array and store JSON
      percentage_per_year_month[idx_year] = jsonData;
      selected_per_year_month[idx_year] = selected_months;
      idx_year++;

      // Reset months array and JSON object
      jsonData = {};
      percentage_months = [];
      selected_months = [];
      idx = 0;
    }
  }

    // Handle ticks of ratio
  custom_ticks = {};
  custom_ticks["min"] = 0;
  custom_ticks["max"] = 100;
  custom_ticks["beginAtZero"] = true;

  var myChart = new Chart(ctx, {
                                responsive: true,
                                type: 'line',
                                data: {
                                labels: months,
                                    datasets: percentage_per_year_month,
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
                                        mode: 'label',
                                        callbacks: {
                                          label: function(tooltipItem, data) {
                                              new_label = "Selected edges: " + parseInt(selected_per_year_month[tooltipItem.datasetIndex][tooltipItem.index]);
                                              return new_label;
                                        }
                                      }
                                      },
                                      scales: {
                                        yAxes: [{
                                          ticks: custom_ticks,
                                          scaleLabel: {
                                            display: true,
                                            labelString: yLabel
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

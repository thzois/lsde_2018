var years = [2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017];
var months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sept", "Oct", "Nov", "Dec"]
              
// Creates a line chart for month comparison between months 
function stacked_line_comparison_months(ctx, ctx_2, filename, title, yLabel_1, yLabel_2, xLabel){

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
  globalJSON= {};
  averageJSON= {};

  var global_cc = new Array();
  var average_cc = new Array();

  var global_year_month = new Array();
  var average_year_month = new Array();

  var idx_year = 0;
  var idx = 0;

  colors = ["#f38b4a", "#0582CA", "#963484", "#E5CA1B", "#F8333C", "#3F7D20", "#7B7C7C", "#000000", "#932C2C"]

  for (i = 1; i < data.length; i++) {

    year = String(data[i].split(",")[0]);
    
    global_cc[idx] = Number(data[i].split(",")[2]);
    average_cc[idx] = Number(data[i].split(",")[3]);

    idx++;

    //Switch year - Construct JSON for chart
    if(i%12 == 0 || (year == "2017" && idx == 3)){

      globalJSON["label"] = year;
      globalJSON["data"] = global_cc;
      globalJSON["borderColor"] = colors[idx_year];
      globalJSON["backgroundColor"] = colors[idx_year];
      globalJSON["fill"] = false;
      globalJSON["borderWidth"] = 2;
    
      averageJSON["label"] = year;
      averageJSON["data"] = average_cc;
      averageJSON["borderColor"] = colors[idx_year];
      averageJSON["backgroundColor"] = colors[idx_year];
      averageJSON["fill"] = false;
      averageJSON["borderWidth"] = 2;

      // Hide every line except the first three
      if(year != "2009" && year != "2010" && year != "2011"){
        globalJSON["hidden"] = true;
        averageJSON["hidden"] = true;
      }

      // Change line for the year array and store JSON
      global_year_month[idx_year] = globalJSON;
      average_year_month[idx_year] = averageJSON;
      idx_year++;

      // Reset months array and JSON object
      globalJSON = {};
      averageJSON = {};

      global_cc = [];
      average_cc = [];
      idx = 0;
    }
  }

    var average_cc = new Chart(ctx_2, {
                                responsive: true,
                                type: 'line',
                                data: {
                                labels: months,
                                    datasets:average_year_month
                                  },
                                  options: {
                                      title: {
                                          display: false,
                                          text: title
                                      },
                                      
                                      legend: {
                                          display:false
                                      },
                                      tooltips: {
                                          mode: 'label'
                                      },
                                      scales: {
                                        yAxes: [{
                                          ticks: {
                                            beginAtZero: true,
                                            min: 0,
                                            max: 1
                                          },
                                          afterFit: function(scale) {
                                              scale.width = 45.5 //<-- set value as you wish 
                                          },
                                          scaleLabel: {
                                            display: true,
                                            labelString: yLabel_2
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


  var global_cc = new Chart(ctx, {
                                responsive: true,
                                type: 'line',
                                data: {
                                labels: months,
                                    datasets:global_year_month
                                  },
                                  options: {
                                      title: {
                                          display: true,
                                          text: title
                                      },
                                      
                                      legend: {
                                          position: 'top',
                                          onClick: function(e, legendItem) {
                                              var index = legendItem.datasetIndex;
                                              var ci = this.chart;
                                              var meta = ci.getDatasetMeta(index);

                                              var ci2 = average_cc.chart.controller;
                                              var meta2 = ci2.getDatasetMeta(index);

                                              // See controller.isDatasetVisible comment
                                              meta.hidden = meta.hidden === null? !ci.data.datasets[index].hidden : null;
                                              meta2.hidden = meta2.hidden === null? !ci2.data.datasets[index].hidden : null;

                                              // We hid a dataset ... rerender the chart*/
                                              ci.update();
                                              ci2.update();
                                          }
                                      },
                                      tooltips: {
                                        mode: 'label'
                                      },
                                      scales: {
                                        yAxes: [{
                                          ticks: {
                                            beginAtZero: true,
                                            min: 0,
                                            max: 1
                                          },
                                          scaleLabel: {
                                            display: true,
                                            labelString: yLabel_1
                                          }
                                        }],
                                        xAxes: [{
                                          scaleLabel: {
                                            display: false,
                                            labelString: xLabel
                                          },
                                          ticks: {
                                            fontColor: "#f8f9fa"
                                          }
                                        }]
                                      } 
                                  }
                              });


}

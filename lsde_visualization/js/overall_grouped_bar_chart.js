var years = [2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017];
var months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sept", "Oct", "Nov", "Dec"]
              
// Creates a bar chart to compare teh size of the graph between years 
function overall_grouped_bar_chart(ctx, title, xLabel){

  //files = ["size_year", "diameter_year", "cc_year", "tpr_year", "bridge_year", "conductance_year"]
  files = ["size_year", "cc_year", "tpr_year", "conductance_year"]
  colors = ["#f38b4a", "#3e95cd", "#8e5ea2","#3cba9f","#c45850", "#585858", "#E5CA1B"]

  max_size = 0;
  max_diameter = 0;
  max_cc = 0;
  max_cc_avg = 0;
  max_tpr = 0;
  max_bridge = 0;
  max_conductance = 0;

  jsonData = {};
  
  metric = new Array();
  cc_avg = new Array();
  combined_metrics = new Array();

  max_metric = 0;
  max_metric_2 = 0;

  idx_combined = 0;
  idx_colors = 0;
  idx = 0;

  for(i = 0; i < files.length; i++){

    // Get results file 
    var req = new XMLHttpRequest();  
    req.open('GET', 'results/year/' + files[i] + ".csv", false);   
    req.send();  
    if(req.status == 4 || req.status == 200){  
      // Read data and split to lines
      var data = (req.responseText).match(/[^\r\n]+/g);
    }else{
      alert("Could not load file " + files[i]);
    }

    // We don't need the first line
    for(j = 1; j < data.length; j++){    
      metric[idx] = Number(data[j].split(",")[1]);

      // Get the max metric
      if(max_metric < metric[idx]){
        max_metric = metric[idx];
      }

      // Get also CC average in case of CC
      if(files[i] == "cc_year"){
        cc_avg[idx] = Number(data[j].split(",")[2]);

        // Get the max cc avg
        if(max_metric_2 < cc_avg[idx]){
          max_metric_2 = cc_avg[idx];
        }
      }
      idx++;
    }


    // Store max value for each case to use them
    // later for the custom labeling
    if(files[i] == "size_year"){
      max_size = max_metric;
    }else if(files[i] == "diameter_year"){
      max_diameter = max_metric;
    }else if(files[i] == "cc_year"){
      max_cc = max_metric;
      max_cc_avg = max_metric_2;
    }else if(files[i] == "tpr_year"){
      max_tpr = max_metric;
    }else if(files[i] == "bridge_year"){
      max_bridge = max_metric;
    }else if(files[i] == "conductance_year"){
      max_conductance = max_metric;
    }


    // Scale to 100%
    for(x = 0; x < metric.length; x++){
      metric[x] = (metric[x]*100)/max_metric;

      if(files[i] == "cc_year"){
        cc_avg[x] = (cc_avg[x]*100)/max_metric_2;
      }
    }

    // Construct JSON
    if(files[i] == "cc_year"){
      jsonData["label"] = "CC Global";
      jsonData["data"] = metric;
      jsonData["borderColor"] = colors[idx_colors];
      jsonData["backgroundColor"] = colors[idx_colors];

      combined_metrics[idx_combined] = jsonData;

      idx_colors++;
      idx_combined++;
      jsonData = {};

      jsonData["label"] = "CC Average";
      jsonData["data"] = cc_avg;
      jsonData["borderColor"] = colors[idx_colors];
      jsonData["backgroundColor"] = colors[idx_colors];
    }else{

      if(files[i] == "tpr_year"){
        jsonData["label"] = "TPR";
      }else if(files[i] == "bridge_year"){
        jsonData["label"] = "Bridge Ratio";
      }else{
          jsonData["label"] = files[i].split("_")[0].charAt(0).toUpperCase() + files[i].split("_")[0].slice(1);
      }

      jsonData["data"] = metric;
      jsonData["borderColor"] = colors[idx_colors];
      jsonData["backgroundColor"] = colors[idx_colors];
    }

    // Change file
    combined_metrics[idx_combined] = jsonData;

    idx_colors++;
    idx_combined++;
    idx = 0;
    
    max_metric = 0;
    max_metric_2 = 0;

    jsonData = {};
    metric = [];
    cc_avg = [];
  }

  // LABELS!

  // Create the chart
  var myChart = new Chart(ctx, {
                                responsive: true,
                                type: 'bar',
                                data: {
                                  labels: years,
                                  datasets: combined_metrics
                                },
                                options: {
                                  title: {
                                      display: true,
                                      text: title
                                  },
                                  tooltips: {
                                      callbacks: {
                                          label: function(tooltipItem, data) {

                                              new_label = "";

                                              // Size - INT
                                              if(tooltipItem.datasetIndex == 0){

                                                new_label = "Size: " + parseInt(((tooltipItem.yLabel/100)*max_size));

                                              // CC Global
                                              }else if(tooltipItem.datasetIndex == 1){
                                                
                                                new_label = "CC Global: " + ((tooltipItem.yLabel/100)*max_cc);

                                              // CC Avg 
                                              }else if(tooltipItem.datasetIndex == 2){
                                                
                                                new_label = "CC Average: " + ((tooltipItem.yLabel/100)*max_cc_avg);

                                              // TPR
                                              }else if(tooltipItem.datasetIndex == 3){
                                                
                                                new_label = "TPR: " + ((tooltipItem.yLabel/100)*max_tpr);

                                              }else if(tooltipItem.datasetIndex == 4){
                                                
                                                new_label = "Conductance: " + ((tooltipItem.yLabel/100)*max_conductance);

                                              }

                                              return new_label;
                                          }
                                      }
                                  },
                                  legend: {
                                      position: 'top'
                                  },
                                  scales: {
                                    xAxes: [{
                                      scaleLabel: {
                                        display: true,
                                        labelString: xLabel
                                      },
                                       barPercentage: 0.8,
                                       categoryPercentage: 0.7
                                    }],
                                    yAxes: [{
                                      scaleLabel: {
                                        display: true
                                      },
                                      ticks: {
                                          display: false
                                      }
                                    }]
                                  } 
                                }
                            });
}

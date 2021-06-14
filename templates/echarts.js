    //分段计算
    function fenduans(data){
      var markLineData = [];
      var idx = 0; var tag = 0; var vols = 0;
      for (var i = 0; i < data.times.length; i++) {
          consol.log(data.datas[i])
          if(data.datas[i][5] != 0 ){
              if (tag == 0){
                idx = i;
                continue;
              }
              markLineData.push([{
                  xAxis: idx,
                  yAxis: data.datas[idx][5] == 'l'?(data.datas[idx][3]).toFixed(2):(data.datas[idx][2]).toFixed(2),
                  value: vols
              }, {
                  xAxis: i,
                  yAxis: data.datas[i][5] == 'l'?(data.datas[i][3]).toFixed(2):(data.datas[i][2]).toFixed(2),
              }]);
              idx = i; vols = 0; tag = 1;
          }
       }
      return markLineData;
    }
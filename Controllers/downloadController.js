const fs = require("fs");
const { connectToDatabase } = require("../connection");
const converter = require("json-2-csv");
var json2xls = require("json2xls");
let tempTable;

//For getting the available databases in the server
exports.database = async (req, res, next) => {
  
  try {
    
    const pool = await connectToDatabase();

    const result = await pool.query( "SELECT name FROM sys.databases" );
 
    const results = result.recordset.map((r) => r.name);

    if (results) {

      res.status(200).json({
        "Total-Databases": results.length,
        results,
        message: "Databases Found!",

      });

    }

  } 
  
  catch (err) {
    
    if (err.originalError && err.originalError.message.includes("The server principal")) {
        
      res.status(401).send("You do not have permission to access this resource.");

    } 
      
    else {
      
      next(err);

    }
  }

};

exports.directory = async (req, res, next) => {
  
  try {
    
    const pool = await connectToDatabase();

    const result = await pool.query( "SELECT name FROM sys.databases" );
 
    const results = `C:\\Users\\${process.env.USERNAME}\\Downloads`;

    if (results) {
      //console.log(results)
      res.status(200).json({
        //"Total-Databases": results.length,
        results
        //message: "Databases Found!",

      });

    }

  } 
  
  catch (err) {
    
    if (err.originalError && err.originalError.message.includes("The server principal")) {
        
      res.status(401).send("You do not have permission to access this resource.");

    } 
      
    else {
      
      next(err);

    }
  }

};


//For getting the available schemas of the database
exports.schema = async (req, res, next) => {
  
  try {
    
    let dbname = req.body.dbname;
    const pool = await connectToDatabase();

    const results = await pool.query( `USE ${dbname}; SELECT * FROM SYS.SCHEMAS ORDER BY NAME ;` );

    const result = results.recordset.map((r) => r.name);
    
    if (result) {

      res.status(200).json({
        "Total-Schemas": result.length,
        result,
        message: "Schemas Found!",

      });

    }

  } 
  
  catch (err) {
    
    if (err.originalError && err.originalError.message.includes("The server principal")) {
      
      res.status(401).send("You do not have permission to access this resource.");

    }
    
    else {
      
      next(err);

    }
  }

};

//For getting the tables in the selected database
exports.tableByDb = async (req, res, next) => {
  
  try {
    
    let dbname = req.body.dbname;
    let schema = req.body.schema;
         
    const pool = await connectToDatabase();

    const results = await pool.query(`USE ${dbname}; SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_CATALOG ='${dbname}' AND TABLE_SCHEMA = '${schema}' ORDER BY TABLE_NAME;`);

    const result = results.recordset.map((table) => table["TABLE_NAME"]);

    if (result) {
     
      res.status(200).json({ result, message: "Tables Found!" });
          
    }
        
  } 
  catch (error) {
    
    res.status(500).json({ error, message: "Something went wrong!" });
    
  }

};

exports.downloadSingle = async (req, res, next) => {

  let dbname = req.body.dbname;
  let schema = req.body.schema;
  let tablename = req.body.tablename;
  let separator = req.body.separator;
  let newDirectory = req.body.directory;//`C:\\Users\\${process.env.USERNAME}\\Downloads`;
  let fileType = req.body.fileType;
  let qualifier = req.body.qualifier;
  let userName = req.body.userName;
  let saveName = req.body.saveName;
  let isSave = req.body.isSave;

  const pool = await connectToDatabase();

  try {

    if (isSave == true) {
      
      tempTable =
      "insert into [InsightLanding].[MUFG].[Analytics_COE_Dashboard] (Name, UserName, DatabaseName, SchemaName , TableDetails , Format , Qualifier , Separator ) values('" +
      saveName + "','" + userName +  "','" + dbname + "','" + schema + "','" + tablename + "','" + fileType + "','" +
      qualifier + "','" + separator + "')";

      await pool.query(tempTable);

    }
    
    const query1=`USE ${dbname}; SELECT * FROM `+ schema +"."+tablename ;
    
    const results = await pool.query(query1);

    let data = JSON.stringify(results);

    let jsonData = JSON.parse(data);
          
    jsonData = jsonData.recordset;

    let datatype=`Use ${dbname}; SELECT COLUMN_NAME,DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA ='${schema}' AND TABLE_NAME ='${tablename}'`;

    datatype = await pool.query(datatype);
    datatype = datatype.recordset;

    let column = datatype.map((column)=>column["COLUMN_NAME"]);
    let data_type = datatype.map((datatype)=>datatype["DATA_TYPE"])

    for(let i =0; i<column.length; i++){
      
      if(data_type[i] == "datetime2"){
          
        for(let j=0; j<jsonData.length; j++){

          if(jsonData[j][column[i]]){

            jsonData[j][column[i]] = jsonData[j][column[i]].replaceAll("T"," ").replaceAll("Z","");

          }

        }

      }
        
    }

    let file;
    let options;
    let isError;

    if (fileType == "csv") {

      file = newDirectory + `/${tablename}.csv`;

      separator = ",";

      options = {
        delimiter: {
          field: ",",
        },
      };

    }

    if (fileType == "tsv") {

      file = newDirectory + `/${tablename}.tsv`;

      separator = "\t";

      options = {
        delimiter: {
          field: "\t",
        },
      };

    }

    if (fileType == "txt") {

      file = newDirectory + `/${tablename}.txt`;

      options = {
        delimiter: {
          field: separator.toString(),
        },
      };

    }

    if (fileType == "json") {

      file = newDirectory + `/${tablename}.json`;

      let data1 = JSON.stringify(jsonData, null, " ");

      fs.writeFile(file, data1, "utf8", async function (err) {
                
        isError = false;

        if (err){

          isError = true;

          success = "Failed";

          description = err.message.toString().replaceAll("'","`").replaceAll("\\","\\\\")
                 
          let log_query= "insert into InsightLanding.MUFG.Analytics_COE_DownloadLog(UserName,DatabaseName,SchemaName,TableName,Status,Description) values('"+ userName+"','"+ dbname +"','"+schema+"','"+tablename+"','"+ success+"','" + description +"') " 

          await pool.query(log_query);
                
          res.status(401).json({message: description});

        }

        if(isError == false){
                  
          success = "Success";

          description = "File `"+ tablename+"."+ fileType +"` has been downloaded successfully"

          let log_query= "insert into InsightLanding.MUFG.Analytics_COE_DownloadLog(UserName,DatabaseName,SchemaName,TableName,Status,Description) values('"+ userName+"','"+ dbname +"','"+schema+"','"+tablename+"','"+ success+"','" + description +"') " 

          await pool.query(log_query);

          res.status(201).json({message: description});

        }

      });

      return;

    }

    if (fileType == "xls") {

      file = newDirectory + `/${tablename}.xlsx`;

      var xls = json2xls(jsonData);           
 
      fs.writeFile(file, xls, "binary", async function (err) {
                
        isError = false;
                
        if (err) {
                  
          isError = true

          success = "Failed";

          description = err.message.toString().replaceAll("'","`").replaceAll("\\","\\\\")
                  
          let log_query= "insert into InsightLanding.MUFG.Analytics_COE_DownloadLog(UserName,DatabaseName,SchemaName,TableName,Status,Description) values('"+ userName+"','"+ dbname +"','"+schema+"','"+tablename+"','"+ success+"','" + description +"') " 

          await pool.query(log_query);
                
          res.status(401).json({message: description});

        }

        if(isError == false){
                  
          success = "Success";

          description = "File `"+ tablename+"."+ fileType +"` has been downloaded successfully"

          let log_query= "insert into InsightLanding.MUFG.Analytics_COE_DownloadLog(UserName,DatabaseName,SchemaName,TableName,Status,Description) values('"+ userName+"','"+ dbname +"','"+schema+"','"+tablename+"','"+ success+"','" + description +"') " 

          await pool.query(log_query);

          res.status(201).json({message: description});

        }

      });

      return;

    }

    if(fileType == "csv" || fileType == "tsv" || fileType == "txt"){

      let json2csvCallback = function (err, csv, fields) {
              
        if (err) {

          console.log("-------single_file_error_1-------", err);
          throw err;

        }

        if (qualifier) {

          csv = csv.split(separator).map((key) => qualifier + key + qualifier)
          .join(separator);

          let n = csv.split("\n").length;

          csv = csv.split("\n").map((key, index) => {

            if (index === 0) {

              return key + qualifier;

            } 
                    
            else if (index === n - 1) {

              return qualifier + key;

            } 
                    
            else {

              return qualifier + key + qualifier;

            }

          }).join("\n");

        }

        if(fileType == "txt"){
          
          if(separator.toString()== "")
          csv = csv.replaceAll(",","")

        }

        fs.writeFile(file, csv, "utf8", async function (err) {
                
          isError = false;

          if (err) {

            isError = true;
          
            success = "Failed";

            description = err.message.toString().replaceAll("'","`").replaceAll("\\","\\\\")                  

            let log_query= "insert into InsightLanding.MUFG.Analytics_COE_DownloadLog(UserName,DatabaseName,SchemaName,TableName,Status,Description) values('"+ userName+"','"+ dbname +"','"+schema+"','"+tablename+"','"+ success+"','" + description +"') " 

            await pool.query(log_query);
                
            res.status(401).json({message: description});

          }

          if(isError == false){

            success = "Success";

            description = "File `"+ tablename+"."+ fileType +"` has been downloaded successfully"

            let log_query= "insert into InsightLanding.MUFG.Analytics_COE_DownloadLog(UserName,DatabaseName,SchemaName,TableName,Status,Description) values('"+ userName+"','"+ dbname +"','"+schema+"','"+tablename+"','"+ success+"','" + description +"') " 

            await pool.query(log_query);

            res.status(201).json({message: description});

          }

        });
      };

      converter.json2csv(jsonData, json2csvCallback, options);

    }  
              
  } 
  
  catch (error) {

    console.log(error)
    success = "Failed";    

    let log_query= "insert into InsightLanding.MUFG.Analytics_COE_DownloadLog(UserName,DatabaseName,SchemaName,TableName,Status,Description) values('"+ userName+"','"+ dbname +"','"+schema+"','"+tablename+"','"+ success+"','" + error +"') " 

    await pool.query(log_query);

    res.status(401).json({ message:error.toString() });

  }

};

//For downloading Multiple files
exports.downloadMultiple = async (req, res, next) => {

  let dbname = req.body.dbname;
  let schema = req.body.schema;
  let tablename = req.body.tablename;
  let newDirectory = req.body.directory;//`C:\\Users\\${process.env.USERNAME}\\Downloads`;
  let fileType = req.body.fileType;
  let qualifier = req.body.qualifier;
  let separator = req.body.separator;
  let userName = req.body.userName;
  let saveName = req.body.saveName;
  let isSave = req.body.isSave;

  
  const pool = await connectToDatabase();

  try {

    if (tablename.length > 1) {

      tablename = tablename.split(",");

    }

    //For downloading files one by one
    for (i = 0; i < tablename.length; i++) {
          
      let count = tablename.length;
      let cnt = i;
      let isError;

      if (tablename.length >= 1) {

        let query = `USE ${dbname}; SELECT * FROM `+ schema +"."+tablename[i];
            
        const results = await pool.query(query);

        let tempTableName = tablename[i];

        let data = JSON.stringify(results);

        let jsonData = JSON.parse(data);

        jsonData = jsonData.recordset;
              
        let datatype=`Use ${dbname}; SELECT COLUMN_NAME,DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA ='${schema}' AND TABLE_NAME ='${tablename[i]}'`;

        datatype = await pool.query(datatype);
        datatype = datatype.recordset;
            
        let column = datatype.map((column)=>column["COLUMN_NAME"]);
        let data_type = datatype.map((datatype)=>datatype["DATA_TYPE"]);
            
        for(let i =0; i<column.length; i++){
      
          if(data_type[i] == "datetime2"){
              
            for(let j=0; j<jsonData.length; j++){
    
              if(jsonData[j][column[i]]){
    
                jsonData[j][column[i]] = jsonData[j][column[i]].replaceAll("T"," ").replaceAll("Z","");
    
              }
    
            }
    
          }

          if(data_type[i] == "date"){
              
            for(let j=0; j<jsonData.length; j++){
    
              if(jsonData[j][column[i]]){
    
                jsonData[j][column[i]] = jsonData[j][column[i]].split("T")[0];
    
              }
    
            }
    
          }
            
        }

        let file;
        let options;

        if (fileType == "csv") {

          file = newDirectory + `/${tempTableName}.csv`;

          separator = ",";

          options = {
            delimiter: {
              field: ",",
            },
          };

        }

        if (fileType == "tsv") {

          file = newDirectory + `/${tempTableName}.tsv`;

          separator = "\t";

          options = {
            delimiter: {
              field: "\t",
            },
          };

        }

        if (fileType == "txt") {

          file = newDirectory + `/${tempTableName}.txt`;

          options = {
            delimiter: {
              field: separator,
            },
          };

        }
                
        if (fileType == "json") {

          file = newDirectory + `/${tempTableName}.json`;

          let data1 = JSON.stringify(jsonData, null, " ");

          fs.writeFile(file, data1, "utf8", async function (err) {
                    
            isError = false;

            if (err) {
                      
              isError = true; 
                                       
              filePaths.push(file);

              if (count == filePaths.length) {

                //To capture the error in the log table if any error occured while writing

                success = "Failed";

                description = err.message.toString().replaceAll("'","`").replaceAll("\\","\\\\")
                        
                let log_query= "insert into InsightLanding.MUFG.Analytics_COE_DownloadLog(UserName,DatabaseName,SchemaName,TableName,Status,Description) values('"+ userName+"','"+ dbname +"','"+schema+"','"+tablename+"','"+ success+"','" + description +"') " 

                await pool.query(log_query);
                      
                return res.status(401).json({message: description});

              }  

            } 

            //To capture the download log
            if(cnt == tablename.length-1 && isError == false){

              //To save the download details in the dashboard
              if (isSave == true) {
      
                tempTable =
                "insert into [InsightLanding].[MUFG].[Analytics_COE_Dashboard] (Name, UserName, DatabaseName, SchemaName , TableDetails , Format , Qualifier , Separator ) values('" +
                saveName + "','" + userName +  "','" + dbname + "','" + schema + "','" + tablename + "','" + fileType + "','" +
                qualifier + "','" + separator + "')";

                await pool.query(tempTable);

              }
            
              success = "Success";
          
              description = "Files `"+ tablename.toString().replaceAll(",","."+fileType+", ")+"."+ fileType +"` have been downloaded successfully"
          
              let log_query= "insert into InsightLanding.MUFG.Analytics_COE_DownloadLog(UserName,DatabaseName,SchemaName,TableName,Status,Description) values('"+ userName+"','"+ dbname +"','"+schema+"','"+tablename+"','"+ success+"','" + description +"') " 

              await pool.query(log_query);

              res.status(201).json({message: description});

            }
                  
          });

        }

        if (fileType == "xls") {

          file = newDirectory + `/${tempTableName}.xlsx`;

          var xls = json2xls(jsonData);

          fs.writeFile(file, xls, "binary", async function (err) {
                    
            isError = false;
            let filePaths;
            if (err) {
                                       
              filePaths.push(file);

              isError = true;

              if (count == filePaths.length) {

                success = "Failed";

                description = err.message.toString().replaceAll("'","`").replaceAll("\\","\\\\")                        

                let log_query= "insert into InsightLanding.MUFG.Analytics_COE_DownloadLog(UserName,DatabaseName,SchemaName,TableName,Status,Description) values('"+ userName+"','"+ dbname +"','"+schema+"','"+tablename+"','"+ success+"','" + description +"') " 

                await pool.query(log_query);
                      
                return res.status(401).json({message: description});

              }                                           
            } 

            if(cnt == tablename.length-1 && isError == false){

              //To save the download details in the dashboard
              if (isSave == true) {
      
                tempTable =
                "insert into [InsightLanding].[MUFG].[Analytics_COE_Dashboard] (Name, UserName, DatabaseName, SchemaName , TableDetails , Format , Qualifier , Separator ) values('" +
                saveName + "','" + userName +  "','" + dbname + "','" + schema + "','" + tablename + "','" + fileType + "','" +
                qualifier + "','" + separator + "')";

                await pool.query(tempTable);

              }
            
              success = "Success";
          
              description = "Files `"+ tablename.toString().replaceAll(",","."+fileType+", ")+"."+ fileType +"` have been downloaded successfully"
                      
              let log_query= "insert into InsightLanding.MUFG.Analytics_COE_DownloadLog(UserName,DatabaseName,SchemaName,TableName,Status,Description) values('"+ userName+"','"+ dbname +"','"+schema+"','"+tablename+"','"+ success+"','" + description +"') " 

              await pool.query(log_query);

              res.status(201).json({message: description});

            }

          });
                  
        }

        if(fileType == "csv" || fileType == "tsv" || fileType == "txt"){
                  
          let json2csvCallback = function (err, csv) {
                  
            if (err) throw err;

            if (qualifier) {
                    
              csv = csv.split(separator).map((key) => qualifier + key + qualifier)
              .join(separator);

              let n = csv.split("\n").length;

              csv = csv.split("\n").map((key, index) => {
                      
                if (index === 0) {

                  return key + qualifier;

                } 
                        
                else if (index === n - 1) {

                  return qualifier + key;

                } 
                        
                else {

                  return qualifier + key + qualifier;

                }

              }).join("\n");

            }

            if(fileType == "txt"){
          
              if(separator.toString()== "")
              csv = csv.replaceAll(",","")
    
            }

            fs.writeFile(file, csv, "utf8", async function (err) {

              isError = false;
                    
              if (err) {
                      
                isError = true;
                                       
                filePaths.push(file);

                success = "Failed";

                description = err.message.toString().replaceAll("'","`").replaceAll("\\","\\\\")                

                let log_query= "insert into InsightLanding.MUFG.Analytics_COE_DownloadLog(UserName,DatabaseName,SchemaName,TableName,Status,Description) values('"+ userName+"','"+ dbname +"','"+schema+"','"+tablename+"','"+ success+"','" + description +"') " 

                await pool.query(log_query);
                      
                res.status(401).json({message: description});
                                          
              }  

              if(cnt == tablename.length-1 && isError == false){

                //To save the download details in the dashboard
                if (isSave == true) {
      
                  tempTable =
                  "insert into [InsightLanding].[MUFG].[Analytics_COE_Dashboard] (Name, UserName, DatabaseName, SchemaName , TableDetails , Format , Qualifier , Separator ) values('" +
                  saveName + "','" + userName +  "','" + dbname + "','" + schema + "','" + tablename + "','" + fileType + "','" +
                  qualifier + "','" + separator + "')";

                  await pool.query(tempTable);

                }
            
                success = "Success";
          
                description = "Files `"+ tablename.toString().replaceAll(",","."+fileType+", ")+"."+ fileType +"` have been downloaded successfully"
          
                let log_query= "insert into InsightLanding.MUFG.Analytics_COE_DownloadLog(UserName,DatabaseName,SchemaName,TableName,Status,Description) values('"+ userName+"','"+ dbname +"','"+schema+"','"+tablename+"','"+ success+"','" + description +"') " 

                await pool.query(log_query);

                res.status(201).json({message: description});

              }  

            });

          };

          converter.json2csv(jsonData, json2csvCallback, options);

        }
         
      } 

    }  

  } 
  
  catch (error) {

    success = "Failed";    

    let log_query= "insert into InsightLanding.MUFG.Analytics_COE_DownloadLog(UserName,DatabaseName,SchemaName,TableName,Status,Description) values('"+ userName+"','"+ dbname +"','"+schema+"','"+tablename+"','"+ success+"','" + error +"') " 

    await pool.query(log_query);

    res.status(401).json({ message:error.toString() });

  }

};


exports.dashboard = async (req, res, next) => {
  
  const pool = await connectToDatabase();
    
  let log_query = `USE InsightLanding; select * from [MUFG].[Analytics_COE_Dashboard]`;
      
  const results = await pool.query(log_query);

  let result = results.recordset;

  result = JSON.parse(JSON.stringify(result));

  if (result) {
        
    res.status(200).json({result,});

  }

};

exports.download_log = async (req, res) => {
  
  const pool = await connectToDatabase();
    
  let log_query = `USE InsightLanding; select * from [MUFG].[Analytics_COE_DownloadLog]`;
      
  const results = await pool.query(log_query);

  let result = results.recordset;

  result = JSON.parse(JSON.stringify(result));

  if (result) {
        
    res.status(200).json({result,});

  }

};
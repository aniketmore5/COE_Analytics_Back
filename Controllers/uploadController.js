const { connectToDatabase } = require("../connection");
const fs = require("fs").promises;
const csv = require("csvtojson");
const excelToJson = require("convert-excel-to-json");

exports.getTable = async (req,res) =>{
  
  const dbname = req.body.dbname;
  const schema = req.body.schema;
  const tableName = req.body.tablename;

  const pool = await connectToDatabase();

  try {

    let query = `Use ${dbname}; SELECT Table_name FROM information_schema.tables WHERE TABLE_SCHEMA ='${schema}' AND TABLE_NAME ='${tableName}'`;

    const result = await pool.query(query);
          
    const db_table = result.recordset.map((r) => r["Table_name"]);

    if(result){
          
      res.status(200).json({ db_table, message: "Tables Found!" });
      console.log(db_table);

    }

  }

  catch(error){

    console.log(error);
    await pool.query(log_query);

    res.status(401).json({message:error.sqlMessage});

  }

}

exports.createTable = async (req, res) => {

  const dbname = req.body.dbname;
  const tableName = req.body.tablename;
  const schema = req.body.schema;
  const userName = req.body.user;
  const script = req.body.script;
  let success ;

  const pool = await connectToDatabase();

  try {
      
  
    let results = await pool.query(script);
    /////let results = await pool.query(query);
         /* if(err) {

            success = "Failed";
          
            let log_query= "insert into test.log_table(UserName,DatabaseName,TableName,Status,Description) values('"+ userName+"','"+ dbname +"','"+tableName+"','"+ success+"','" + err.sqlMessage.replaceAll("'","`") +"') " 
          
            connection.query(log_query,function(err,result){
            
              if (err) throw err;

            })
          
            res.status(401).json({message:err.sqlMessage});
    }*/

    if(results){

      description = `Table ${dbname}.${schema}.${tableName} has been created`;
      console.log(script)

      let log_query= "insert into InsightLanding.MUFG.Analytics_COE_TableLog(UserName,DatabaseName,SchemaName,TableName,Description,CreationScript) values('"+ userName+"','"+ dbname +"','"+schema+"','"+tableName+"','"+ success+"','" + description +"') " 
          
      //let log_results = await pool.query(log_query);
            
      res.status(201).json({message:"Table ["+ schema +"].["+ tableName +"] created successfully"});

    }

  } 

  catch(error) {   

    success = "Failed";
    console.log(error)     
    let log_query= "insert into InsightLanding.MUFG.Analytics_COE_UploadLog(UserName,DatabaseName,SchemaName,TableName,Status,Description) values('"+ userName+"','"+ dbname +"','"+schema+"','"+tableName+"','"+ success+"','" + description +"') " 

    await pool.query(log_query);

    res.status(401).json({message:error.sqlMessage});
  
  }

};


exports.getColumns = async (req, res) => {

  let file = req.files.file;
  let type = file.name.split(".")[1];
  let separator = req.body.separator;
  let qualifier = req.body.qualifier;

  const pool = await connectToDatabase();

  try{

    if (type == "csv") {

      let csvRow = await csv().fromString(file.data.toString());
      json_array = csvRow;
      json_arr = await csv().fromString(file.data.toString());

    }

    if (type == "json") {

      json_array = JSON.parse(file.data.toString());
  
      json_arr = JSON.parse(file.data);

    }

    if (type == "xlsx") {
    
      await fs.writeFile("temp.xlsx", file.data);
      json_array = excelToJson({ sourceFile: "temp.xlsx" })["Sheet 1"];
      json_array.shift();
      json_arr = excelToJson({ sourceFile: "temp.xlsx" })["Sheet 1"];

    }

    if (type == "txt" || type == "tsv") {
      
      json_array = file.data.toString().split("\n");
      json_array.shift();
      json_arr = file.data.toString().split("\n");

    }

    let valueArray;
    let valueArrays = [];

    for (let i in json_array) {

      if (type == "txt" || type == "tsv") {

        if (type == "tsv") separator = "\t";

        valueArray = json_array[i].split(separator)
        .map((each) => each.replaceAll(qualifier, ""));
        file_column = json_arr[0].split(separator)
        .toString().replaceAll(qualifier, "");

        file_qualifier = json_arr[0].split(separator).toString().substr(0, 1);

      }

      if (type == "xlsx") {

        valueArray = Object.values(json_array[i]);

        file_column = Object.values(json_arr[0]).toString();

      }

      if (type == "json") {

        valueArray = Object.values(json_array[i]);

        file_column = Object.keys(json_arr[0]).toString();

      }

      if (type == "csv") {

        valueArray = Object.values(json_array[i]);

        file_column = Object.keys(json_arr[0]).toString().replaceAll(qualifier, "");

        file_qualifier = Object.keys(json_arr[0]).toString().substring(0, 1);

      }

      valueArrays.push(valueArray);

    }

    res.json({
    columns: file_column.split(","),
    data: valueArrays,    
    });

  }

  catch(error) {   
    console.log(error)
    success = "Failed";
          
    //let log_query= "insert into InsightLanding.MUFG.Analytics_COE_UploadLog(UserName,DatabaseName,SchemaName,TableName,Status,Description) values('"+ userName+"','"+ dbname +"','"+schema+"','"+tableName+"','"+ success+"','" + description +"') " 

    //await pool.query(log_query);

    res.status(401).json({message:error.sqlMessage});
  
  }

};


exports.upload = async (req, res) => {
  let file = req.files.file;
  let type = file.name.split(".")[1];
  let json_array;
  let json_arr;
  let file_column = [];
  let table_column = [];
  let file_qualifier;
  let table = req.body.tablename;
  let database = req.body.dbname;
  let schema = req.body.schema;
  let qualifier = req.body.qualifier;
  let seperator = req.body.seperator;
  let userName = req.body.user;
  let description;

  const pool = await connectToDatabase();

  try {

    if (type == "csv") {

      let csvRow = await csv().fromString(file.data.toString());

      json_array = csvRow;

      json_arr = await csv().fromString(file.data.toString());

    }

    if (type == "json") {

      json_array = JSON.parse(file.data.toString());
  
      json_arr = JSON.parse(file.data);
  
    }

    if (type == "xlsx") {

      await fs.writeFile("temp.xlsx", file.data);

      json_array = excelToJson({ sourceFile: "temp.xlsx" })["Sheet 1"];
      json_array.shift();

      json_arr = excelToJson({ sourceFile: "temp.xlsx" })["Sheet 1"];

    }

    if (type == "txt" || type == "tsv") {

      json_array = file.data.toString().split("\n");
      json_array.shift();

      json_arr = file.data.toString().split("\n");

    }
        
    table_col = `Use ${database}; SELECT Column_name FROM information_schema.columns WHERE TABLE_SCHEMA ='${schema}' AND TABLE_NAME ='${table}'`;
      
    const results = await pool.query(table_col); 
          
    table_column = results.recordset.map((r) => r["Column_name"]).toString();
    console.log(table_column)

    let valueArray;                                                                         
        
    var successRecords = 0;
    var failureRecords = 0;

    for (let i in json_array) {
      
      let count = i;

      if (type == "txt" || type == "tsv") {

        if (type == "tsv") seperator = "\t";
              
        valueArray = json_array[i].split(seperator)
        .map((each) => each.replaceAll(qualifier, ""));

        file_column = json_arr[0].split(seperator)
        .toString().replaceAll(qualifier, "");

        file_qualifier = json_arr[0].split(seperator)
        .toString().substr(0, 1);

      }

      if (type == "xlsx") {

        valueArray = Object.values(json_array[i]);

        file_column = Object.values(json_arr[0]).toString();

        file_qualifier=qualifier='';

      }

      if (type == "json") {

        valueArray = Object.values(json_array[i]);

        file_column = Object.keys(json_arr[0]).toString();
        console.log(Object.values(json_arr))
        file_qualifier = qualifier='';

      }

      if (type == "csv") {

        valueArray = Object.values(json_array[i]);

        file_column = Object.keys(json_arr[0]).toString()
        .replaceAll(qualifier, "");

        file_qualifier = Object.keys(json_arr[0])
        .toString().substring(0, 1);

      }

      if (file_qualifier !== qualifier.substr(0,1)) {

        success = "Failed";
  
        description = "Qualifier entered does not match with file qualifier"
        
        let log_query= "insert into InsightLanding.MUFG.Analytics_COE_UploadLog(UserName,DatabaseName,SchemaName,TableName,Status,Description) values('"+ userName+"','"+ database +"','"+schema+"','"+table+"','"+ success+"','" + description +"') " 
            
        await pool.query(log_query);
            
        res.status(401).json({message:"Please Check the Qualifier entered"});
  
        return;
  
      }

      
      if (table_column !== file_column) {
            
        success = "Failed";

        description = "File column does not match with Database Table column"
      
        let log_query= "insert into InsightLanding.MUFG.Analytics_COE_UploadLog(UserName,DatabaseName,SchemaName,TableName,Status,Description) values('"+ userName+"','"+ database +"','"+schema+"','"+table+"','"+ success+"','" + description +"') " 
          
        await pool.query(log_query)

        res.status(401).json({message:"File column doesn't match with the Table column"});

        return;

      }

      if (table_column === file_column) {

        let Values = valueArray
        .map((value) => `'${value.toString().replaceAll(qualifier, "")}'`)
        .join(",");

        let query = `Use ${database}; insert into ${schema}.${table} Values(${Values})`;
            
        await pool.query(query,async (error,results)=>{
          
          if(error){
            console.log(error);

            description= error.message.replaceAll("'","`");

            let log_query= "insert into InsightLanding.MUFG.Analytics_COE_Upload_ErrorLog(UserName,DatabaseName,SchemaName,TableName,Description) values('"+ userName+"','"+ database +"','"+schema+"','"+table+"','" + description +"') " 
          
            await pool.query(log_query);

            failureRecords= failureRecords + 1;  
          }
          
          if(results){

            successRecords= successRecords + 1;

          }
        

                      
         /* if (err) {
                 
            error_desc= err.sqlMessage.toString().replaceAll("'","");
              
            Values= Values+",'"+error_desc+"'"
              
            error_file_column=file_column+",error_description";             
              
            let query1=`insert into ${database}.${table}_error(`+error_file_column +`) Values(${Values})`;
           
            let query2 = "insert into test.error_log(UserName,DatabaseName,TableName,FileName,ErrorDescription) values('"+ userName+"','"+ database +"','"+table+"','"+ file.name +"','" + err.toString().replaceAll("'","`") +"') " 

            connection.query(query2, function (err) {

              if(err) {

                let log_query= "insert into test.error_log(UserName,DatabaseName,TableName,FileName,ErrorDescription) values('"+ userName+"','"+ database +"','"+table+"','"+ file.name +"','" + err.toString().replaceAll("'","`") +"') "
                                                         
                connection.query(log_query,function(err,result){
                  
                  if (err) throw err;

                })
                   
              };

            }) ;  
                
            failureRecords= failureRecords + 1;   

          } */
            
 

            
          if(count == json_array.length-1){

            success = "Success";

            description = "File `"+ file.name +"` has been uploaded successfully.\n Success Records : "+ successRecords +"\n Failure Records : "+ failureRecords
          
            let log_query= "insert into InsightLanding.MUFG.Analytics_COE_UploadLog(UserName,DatabaseName,SchemaName,TableName,Status,Description) values('"+ userName+"','"+ database +"','"+schema+"','"+table+"','"+ success+"','" + description +"') " 
              
            await pool.query(log_query);

            res.status(201).json({message: "Success Records: "+successRecords+ "\n Failure Records: " +failureRecords});
  
            return;

          }
        })
      } 

    }

    console.log("File Type: " + type);
    console.log("Table Columns: " + table_column);
    console.log("File Columns: " + file_column);
    console.log("File Qualifier: " + file_qualifier);
    console.log("Qualifier: " + qualifier);

   


  } 
    
  catch (error) {
    console.log(error)
    success = "Failed";

    description = error.toString().replaceAll("'","`");
      
    let log_query= "insert into InsightLanding.MUFG.Analytics_COE_UploadLog(UserName,DatabaseName,SchemaName,TableName,Status,Description) values('"+ userName+"','"+ database +"','"+schema+"','"+table+"','"+ success+"','" + description +"') " 
          
    await pool.query(log_query)

    res.status(401).json({ message: error.message });

  }

};

exports.log = async (req, res) => {

  const pool = await connectToDatabase();

  let result = await pool.query("select * from InsightLanding.MUFG.Analytics_COE_TableLog")
      
  result = JSON.parse(JSON.stringify(result.recordset));

  if (result) {

    res.status(200).json({result});

  }
    
  

};

exports.upload_log = async (req, res) => {
  
  const pool = await connectToDatabase();
    
  let result = await pool.query("select * from InsightLanding.MUFG.Analytics_COE_UploadLog")

  result = JSON.parse(JSON.stringify(result.recordset));

  if (result) {
        
    res.status(200).json({result,})

  }

};





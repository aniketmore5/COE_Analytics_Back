const mysql = require("mssql");
const fs = require("fs");
const ws = fs.createWriteStream("mydb.csv");



exports.connectionU = async (req, res, next) => {
    let servername = req.body.host;
    
    const username = req.body.user;
    const password1 = req.body.password;

    servername = servername.replaceAll("\\","\\\\")
   
    console.log(servername)
   



    // writeFile function with filename, content and callback function
    fs.writeFile('connection.js', `const mysql = require("mssql");

    const config = {
        server: "${servername}",
        user: "${username}",
        password: "${password1}",
        options: {
            trustServerCertificate: true
          },
          connectionTimeout: 30000 
    
        };
        const connectToDatabase = async () => {
            try {
              const pool = await mysql.connect(config);
              console.log("Connected to MSSQL database");
              return pool;
            } catch (err) {
              console.error("Error connecting to MSSQL database", err);
              throw err;
            }
          };
          
                                        
    module.exports= { connectToDatabase }`, function (err) {
        if (err) throw err;
        ws.on('finish', 
            res.status(200)
                .json({

                    message: "Server setup!",
                })

        );
        console.log('File is created successfully.');
    })
};


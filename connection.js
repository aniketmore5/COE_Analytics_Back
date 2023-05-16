const mysql = require("mssql");

    const config = {
        server: "192.168.180.30\\analyticsdv2",
        user: "anikmore",
        password: "Password123",
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
          
                                        
    module.exports= { connectToDatabase }
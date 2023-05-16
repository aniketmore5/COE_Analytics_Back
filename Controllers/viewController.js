const { connection } = require('../connection');

exports.database = async (req, res, next) => {
    connection.connect(function (err) {

        connection.query("show databases", function (err, result, fields) {
            if (err) throw err;
            const results = result.map((r) => r["Database"]);
            if (results) {
                res
                    .status(200)
                    .json({
                        "Total-Databases": result.length,
                        result,
                        message: "Databases Found!",
                    });

            }
        });
    });
};

exports.tables = async (req, res, next) => {
    try {

        let dbname = req.body.dbname;

        connection.connect(function (err) {
            let query1 = "SHOW TABLES FROM " + dbname;

            connection.query(query1, function (err, result, fields) {
                if (err) throw err;

                const results = result.map((table) => table["Tables_in_" + dbname]);

                if (results) {
                    res.status(200).json({ results, message: "Tables Found!" });

                }
            });
        });
        //})
    } catch (error) {
        res.status(500).json({ error, message: "Something went wrong!" });
    }
};

exports.columns = async (req, res, next) => {
    try {

        let dbname = req.body.dbname;
        let tname = req.body.table;


        connection.connect(function (err) {

            let query4 = `show columns from ${dbname}.${tname}`

            connection.query(query4, function (err, result, fields) {
                if (err) throw err;

                const results = result.map((table) => table["Field"]);

                if (results) {
                    res.status(200).json({ results, message: "columns Found!" });


                }

            });
        });

        //})
    } catch (error) {
        res.status(500).json({ error, message: "Something went wrong!" });
    }
};
exports.seccolumns = async (req, res, next) => {
    try {

        let query = req.body.query;
        


        connection.connect(function (err) {

            let query4 = `show columns from ${query};`

            connection.query(query4, function (err, result, fields) {
                if (err) throw err;

                const results = result.map((table) => table["Field"]);

                if (results) {
                    res.status(200).json({ results, message: "columns Found!" });


                }

            });
        });

        //})
    } catch (error) {
        res.status(500).json({ error, message: "Something went wrong!" });
    }
};

exports.ViewCreate = async (req, res, next) => {
    try {
        let script = req.body.script;
        connection.connect(function (err) {

            let query4 = `call create_view_test_database.createView('${script}')`;

            connection.query(query4, function (err, result, fields) {
                if (err) throw err;
                if (result) {
                    res.status(200).json({ result, message: "view created successfully !" });


                }
            });
        });
    } catch (error) {
        res.status(500).json({ error, message: "Something went wrong!" });
    }
};


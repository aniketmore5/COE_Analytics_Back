const express = require('express');
const Router = express.Router();
const uploadController = require('../Controllers/uploadController');

Router.post('/upload', uploadController.upload);
Router.post('/columns', uploadController.getColumns);
Router.post('/createtable', uploadController.createTable);
Router.get('/log', uploadController.log);
Router.get('/upload_log', uploadController.upload_log);
Router.post('/tableColumns',uploadController.getTable)

module.exports = Router;
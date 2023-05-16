const express = require('express');
const downloadController = require('../Controllers/downloadController');
const Router = express.Router();

Router.get('/databases', downloadController.database);
Router.get('/directory', downloadController.directory);
Router.post('/schema', downloadController.schema);
Router.post('/database', downloadController.tableByDb);
Router.post('/csv/multi', downloadController.downloadMultiple);
Router.post('/xls/multi', downloadController.downloadMultiple);
Router.post('/json/multi', downloadController.downloadMultiple);
Router.post('/tsv/multi', downloadController.downloadMultiple);
Router.post('/txt/multi', downloadController.downloadMultiple);
Router.post('/txt/', downloadController.downloadSingle);
Router.post('/csv/', downloadController.downloadSingle);
Router.post('/tsv/', downloadController.downloadSingle);
Router.post('/json/', downloadController.downloadSingle);
Router.post('/xls/', downloadController.downloadSingle);
Router.get('/dashboard', downloadController.dashboard);
Router.get('/download_log', downloadController.download_log);

module.exports = Router;
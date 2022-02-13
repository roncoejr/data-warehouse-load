const {BigQuery} = require('@google-cloud/bigquery');
const bigquery = new BigQuery();
const path = require('path');
const fs = require('fs');
const { parse } = require('csv-parse');




async function alt_createDataTable(projectId, datasetId, newTableId) {

    newDataSetId = datasetId
  
    var table = {
      tableReference: {
        projectId: projectId,
        datasetId: newDataSetId,
        tableId: newTableId
      },
      schema: {
        fields: [
          {name: 'transactionDate', type: 'DATE'},
          {name: "transactionAmount", type: "FLOAT"},
          {name: "transactionStatus", type: "STRING"}
        ]
      }
    };
  
    var dataset = {
      datasetReference: {
        datasetId: newDataSetId
      }
    };
    async function createTable() {
      const t_opt = {
        schema: table.schema,
        location: 'US',
      };
  
    //  BigQuery.Datasets.insert(dataset, projectId)
  
    // const [bq_dataset] = await bigquery.createDataset(datasetId)
    // console.log(`Dataset: ${bq_dataset.id} created.`);
    //  BigQuery.Tables.insert(table, projectId, datasetId)
    const [bq_table] = await bigquery
                              .dataset(datasetId)
                              .createTable(newTableId, t_opt)
    console.log(`Table: ${bq_table.id} created.`)
    }
    createTable()
  }



  function alt_main() {
    projectId = "midyear-glazing-196002"
    folderName = "rcjGasBillPaymentHistory"
    datasetId = "ds_1644708979307"
    tableId = "table_1644709985185"
  
    alt_createDataTable(projectId, datasetId, tableId)
    // getCSVFiles(folderName, projectId, datasetId, tableId)
  }

  alt_main()

  
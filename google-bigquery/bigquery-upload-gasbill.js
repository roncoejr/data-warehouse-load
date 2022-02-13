const {BigQuery} = require('@google-cloud/bigquery');
const bigquery = new BigQuery();
const path = require('path');
const fs = require('fs');
const { parse } = require('csv-parse');


async function createDataTable(projectId, datasetId, newTableId) {

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

  const [bq_dataset] = await bigquery.createDataset(datasetId)
  console.log(`Dataset: ${bq_dataset.id} created.`);
  //  BigQuery.Tables.insert(table, projectId, datasetId)
  const [bq_table] = await bigquery
                            .dataset(datasetId)
                            .createTable(newTableId, t_opt)
  console.log(`Table: ${bq_table.id} created.`)
  }
  createTable()
}


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



async function insertDataIntoBigQuery(dataPack, projectId, datasetId, tableId) {

 //  job = BigQuery.Jobs.insert(theJob, projectId, theBlob)
  await bigquery
  .dataset(datasetId)
  .table(tableId)
  .insert(dataPack)

  console.log(`Inserted ${dataPack.length} rows.`)
}

function importCSVintoBigQuery(theBlob, rowIncludeFlag, bqProjectId, bqDatasetId, bqTableId) {

  var job = {
    configuration: {
      load: {
        destinationTable: {
          projectId: bqProjectId,
          datasetId: bqDatasetId,
          tableId: bqTableId
        },
        skipLeadingRows: rowIncludeFlag
      }
    }
  };

  // console.log(job + ": " + bqProjectId)
  // job = BigQuery.Jobs.insert(job, bqProjectId, theBlob)
  // insertDataIntoBigQuery(theBlob, bqProjectId, bqTableId, job)
  // console.log(job + ": --- : ")
}

function getCSVContentBlob(theCSVIds, theProjectId, theDataSetId, theTableId, flg_includeHeader) {

  for(i = 0; i < theCSVIds.length; i++) {
      // Open the first CSV file, ingest headings and data
      // var firstFile = DriveApp.getFileById(theCSVIds[i])
      var firstFileData = firstFile.getBlob().setContentType('application/octet-stream')

      if((i == 0) && (flg_includeHeader)) {
        importCSVintoBigQuery(firstFileData, 0, theProjectId, theDataSetId, theTableId)
      }
      else {
        importCSVintoBigQuery(firstFileData, 1, theProjectId, theDataSetId, theTableId)
      }
      console.log(theCSVIds[i] + ": ")
      console.log(firstFileData)
  }

}

function dateFix(m_line) {

        t_month = m_line.substring(m_line.length-10,m_line.length-8)
        t_day = m_line.substring(m_line.length-7,m_line.length-5)
        t_year = m_line.substring(m_line.length-4,m_line.length)

        fixed_date = t_year + "-" + t_month + "-" + t_day

        return fixed_date

}


function amountFix(m_line) {

	console.log("Passed to amount fix: " + m_line)

	n_regex = new RegExp("\\$", "gi")

	m_line_fix = m_line.replace(n_regex, "")

	console.log("Result of amount fix: " + m_line_fix)

	return m_line_fix

}


function getCSVContentArray(theCSVIds, theProjectId, theDataSetId, theTableId, flg_includeHeader, theFolderName) {

  myFolder = path.join(__dirname, theFolderName)
	const firstFileDataCSV = []
	const firstFileDataRow = []
  const dataPack = []
  var insertedFlag = false
  for(i = 0; i < theCSVIds.length; i++) {

        fs.createReadStream(theCSVIds[i])
          .pipe(parse({delimiter: ','}))
          .on('data', function(firstFileDataCSV) {

            firstFileDataRow.push(firstFileDataCSV)	
        })
        .on('end', function() {
                // t_record.push([firstFileDataRow])
          for(j = 0; j < firstFileDataRow.length; j++) {
            if(firstFileDataRow[j][0] != "transactionDate") {
                t_db_transactionDate = dateFix(firstFileDataRow[j][0])
                t_db_transactionAmount = amountFix(firstFileDataRow[j][1])
                dataPack.push({transactionDate: t_db_transactionDate, transactionAmount: parseFloat(t_db_transactionAmount), transactionStatus: firstFileDataRow[j][2]})
            }
          }
            // console.log("[---- " + t_db_transactionDate + " ----]: " + ", " + firstFileDataRow[j][1] + ", " + firstFileDataRow[j][2])
            if(!insertedFlag) {
              insertDataIntoBigQuery(dataPack, theProjectId, theDataSetId, theTableId)
              console.log(dataPack)
              // console.log(dataPack)
                // console.log(dataPack.map(x => x.join(',')).join('\n'))
              insertedFlag = true
            }

          });
  }
}


function getCSVFiles(theFolderName, theProjectId, theDataSetId, theTableId) {
  myFolder = path.join(__dirname, theFolderName)
  var csvIDs = []

  fs.readdir(myFolder, function(err, myFiles) {
      if(err) {
        return console.log('Unable to scan directory: ' + err)
      }
      myFiles.forEach(function(file) {
        csvIDs.push(myFolder + "/" + file)
      });
      getCSVContentArray(csvIDs, theProjectId, theDataSetId, theTableId, false, theFolderName)
      console.log(csvIDs)
  });
}

function main() {
  projectId = "midyear-glazing-196002"
  folderName = "rcjGasBillPaymentHistory"
  datasetId = 'ds_' + new Date().getTime()
  tableId = 'table_' + new Date().getTime()

  createDataTable(projectId, datasetId, tableId)
  getCSVFiles(folderName, projectId, datasetId, tableId)
}

function alt_main() {
  projectId = "midyear-glazing-196002"
  folderName = "rcjGasBillPaymentHistory"
  datasetId = "ds_1644708979307"
  tableId = "table_1644709985185"

  // alt_createDataTable(projectId, datasetId, tableId)
  getCSVFiles(folderName, projectId, datasetId, tableId)
}



function loadTest() {
  projectId = "midyear-glazing-196002"
  folderName = "rcjGasBillPaymentHistory"
  datasetId = "ds_1644708979307"
  tableId = "table_1644416200996"

  // createDataTable(projectId, datasetId, tableId)
  getCSVFiles(folderName, projectId, datasetId, tableId)
}

function connectUp() {


    alt_main()

}

connectUp()


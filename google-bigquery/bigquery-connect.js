const snowflake = require('snowflake-sdk');
const path = require('path');
const fs = require('fs');
const { parse } = require('csv-parse');

function createDataTable(projectId, datasetId, newTableId) {

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

//  BigQuery.Datasets.insert(dataset, projectId)
//  BigQuery.Tables.insert(table, projectId, datasetId)
}

function insertDataIntoSnowflake(theBlob, projectId, tableId, theJob) {

 //  job = BigQuery.Jobs.insert(theJob, projectId, theBlob)
}

function importCSVintoSnowflake(theBlob, rowIncludeFlag, bqProjectId, bqDatasetId, bqTableId) {

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
        importCSVintoSnowflake(firstFileData, 0, theProjectId, theDataSetId, theTableId)
      }
      else {
        importCSVintoSnowflake(firstFileData, 1, theProjectId, theDataSetId, theTableId)
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


function getCSVContentArray(theCSVIds, theProjectId, theDataSetId, theTableId, flg_includeHeader, theFolderName) {

  myFolder = path.join(__dirname, theFolderName)
  let j = 0
  let theBlob = ""
  let t_record = []
  let t_record_sub = []
	var firstFileDataCSV = []
	var firstFileDataRow = []
	var dataPack = []
  for(i = 0; i < theCSVIds.length; i++) {

	fs.createReadStream(theCSVIds[i])
		.pipe(parse({delimiter: ','}))
		.on('data', function(firstFileDataCSV) {

			firstFileDataRow.push(firstFileDataCSV)	
	})
	.on('end', function() {
      		// t_record.push([firstFileDataRow])
		for(j = 0; j < firstFileDataRow.length; j++) {
			t_db_transactionDate = dateFix(firstFileDataRow[j][0])
			dataPack.push([t_db_transactionDate], [firstFileDataRow[j][1]], [firstFileDataRow[j][2]])
			console.log("[---- " + t_db_transactionDate + " ----]: " + ", " + firstFileDataRow[j][1] + ", " + firstFileDataRow[j][2])
		}
		console.log(dataPack.map(x => x.join(',')).join('\n'))
	});
      var regex = new RegExp(('\n,'),'gi')

      // theBlob = Utilities.newBlob(t_record.toString().replace(regex, '\n'), 'application/octet-stream')

      // importCSVintoSnowflake(theBlob, 0, theProjectId, theDataSetId, theTableId)
      console.log(t_record.toString().replace(regex, "\n"))
      t_record_sub = []
      t_record = []

  }


}


function getCSVFiles(theFolderName, theProjectId, theDataSetId, theTableId) {
  myFolder = path.join(__dirname, theFolderName)
  var csvIDs = []

  //if(myFolder.hasNext()) {
   // myFolderOfInterest = myFolder.next()
   // myFolderId = myFolderOfInterest.getId()
   //  console.log(myFolderId)
   //  myFiles = myFolderOfInterest.getFiles()
   //  while(myFiles.hasNext()) {
   //   theFile = myFiles.next()
   //   csvIDs.push(theFile.getId())
   //   console.log(theFile.getName())
   //  }

   fs.readdir(myFolder, function(err, myFiles) {

	if(err) {
		return console.log('Unable to scan directory: ' + err)
	}
	myFiles.forEach(function(file) {
		csvIDs.push(myFolder + "/" + file)
		// console.log(csvIDs);
		// getCSVContentsBlob(csvIDs, theProjectId, theDataSetId, theTableId, false)
   		// console.log(csvIDs.length + " : " )

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


function loadTest() {
  projectId = "midyear-glazing-196002"
  folderName = "rcjGasBillPaymentHistory"
  datasetId = "ds_1644416200996"
  tableId = "table_1644416200996"

  // createDataTable(projectId, datasetId, tableId)
  getCSVFiles(folderName, projectId, datasetId, tableId)
}

function connectUp() {


var connection = snowflake.createConnection( {
	accessUrl: process.env.SNOW_URL,
 	account: process.env.SNOW_ACCOUNT,
	username: process.env.SNOW_USER,
	password: process.env.SNOW_PASS
});

connection.connect(
	function(err, conn) {

		if(err) {
			console.error('Unable to connect: ' + err.message);
		}
		else {
			console.log('Successfully connected to Snowflake.');
			connection_ID = conn.getId();
		}

	}
);

main()

}



connectUp()


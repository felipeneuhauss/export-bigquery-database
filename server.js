// Import the Google Cloud client libraries
const {BigQuery} = require('@google-cloud/bigquery');
const {Storage} = require('@google-cloud/storage');

const bigquery = new BigQuery({projectId: process.env.PROJECT_ID});
const storage = new Storage({projectId: process.env.PROJECT_ID});

async function extractTableToGCS() {
  // Exports my_dataset:my_table to gcs://my-bucket/my-file as raw CSV.

  /**
   * TODO(developer): Uncomment the following lines before running the sample.
   */
  const datasetId = 'hotdata'
  const bucketName = process.env.EXPORT_BUCKET
  console.log('bucketName', bucketName)
  const tableNameList = [
    'tableA',
    'tableB',
    'tableC'
  ]

  // Location must match that of the source table.
  const options = {
    location: 'EU',
  };

  // Export data from the table into a Google Cloud Storage file
  for (const tableName of tableNameList) {
    const [job] = await bigquery
      .dataset(datasetId)
      .table(tableName)
      .extract(storage.bucket(bucketName).file(`${tableName}/${tableName}-*.csv`), options);

    console.log(`Job ${job.id} - ${tableName} created.`);

      // Check the job's status for errors
    const errors = job.status.errors;
    if (errors && errors.length > 0) {
      throw errors;
    }
  }
}

extractTableToGCS()

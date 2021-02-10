const monitoring = require('@google-cloud/monitoring');
const { google } = require('googleapis');
const { BigQuery } = require('@google-cloud/bigquery');

exports.helloPubSub = (event, context) => {
  google.auth.getApplicationDefault(function (err, authClient, projectId) {
    if (err) {
      throw err;
    }

    const metric = "custom.googleapis.com/dataflow/count_bq_writes";
    const metricService = new monitoring.MetricServiceClient();
    const filter = 'metric.type="' + metric + '" resource.type="dataflow_job"';
    const secondsBeforeNow = 60 * 60 * 2; // 2 hours
    const request = {
        name: metricService.projectPath(projectId),
        filter: filter,
        interval: {
            startTime: {
                seconds: Date.now() / 1000 - secondsBeforeNow,
            },
            endTime: {
                seconds: Date.now() / 1000,
            },
        },
        aggregation: {
            alignmentPeriod: {
                seconds: secondsBeforeNow,
            },
            crossSeriesReducer: 'REDUCE_MAX',
            perSeriesAligner: 'ALIGN_MAX'
        }
    };

    metricService.listTimeSeries(request).then(res => {
      console.log('Done writing time series data.', JSON.stringify(res));
      var value = parseFloat(res[0][0].points[0].value.doubleValue);

      const bigquery = new BigQuery();
      var metricsBQTable = "avroset.metric_container"
      insertMetric(metricsBQTable, value).then(r => { console.log("BQ result = " + JSON.stringify(r))});
      async function insertMetric(metricsBQTable, metricValue) {
          const insertSqlQuery =
              `INSERT ${metricsBQTable} (value) VALUES (${metricValue})`;
          return await executeQuery(insertSqlQuery);
      }

      async function executeQuery(sqlQuery) {
          console.log(`Starting an execution of the query: ${sqlQuery}`)
          const options = {
              query: sqlQuery,
              timeoutMs: 100000, // Time out after 100 seconds.
              useLegacySql: false, // Use standard SQL syntax for queries.
          };
          return bigquery.query(options);
      }
    });
  });
};

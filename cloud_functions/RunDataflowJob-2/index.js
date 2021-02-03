const { google } = require('googleapis');

exports.runDataflow2 = (req, res) => {
  google.auth.getApplicationDefault(function (err, authClient, projectId) {
    if (err) {
      throw err;
    }

    const dataflow = google.dataflow({ version: 'v1b3', auth: authClient });

    var datetime = new Date();
    var folderName = datetime.toISOString().slice(0, 10);

    dataflow.projects.locations.templates.create({
      gcsPath: 'gs://beam-beam/templates/rawtobq', 
      projectId: projectId,
      location: 'europe-west3',
      resource: {
        parameters: {
          inputFilePattern: `gs://beam-beam/avroout/${folderName}/*.avro`,
          outputTable: 'planningmeme:avroset.dynamictable',
          writeDisposition: 'WRITE_TRUNCATE',
          createDisposition: 'CREATE_IF_NEEDED'
        },
        environment: {
          tempLocation: 'gs://beam-beam/temp',
          zone: "europe-west3-a"
        },
        jobName: 'Raw to BQ'
      }
    }, function(err, response) {
      if (err) {
        console.error("problem running dataflow template, error was: ", err);
      }
      console.log("Dataflow template response: ", response);
    });
  });
};

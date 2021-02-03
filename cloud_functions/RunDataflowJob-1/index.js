const { google } = require('googleapis');

exports.runDataflow1 = (req, res) => {
  google.auth.getApplicationDefault(function (err, authClient, projectId) {
    if (err) {
      throw err;
    }

    const dataflow = google.dataflow({ version: 'v1b3', auth: authClient });

    var datetime = new Date();
    var folderName = datetime.toISOString().slice(0, 10);

    dataflow.projects.locations.templates.create({
      gcsPath: 'gs://beam-beam/templates/sqltoavro', 
      projectId: projectId,
      location: 'europe-west3',
      resource: {
        parameters: {
          username: 'postgres',
          password: 'b4KnhkDkL8p9dFfz',
          driverClassName: 'org.postgresql.Driver',
          url: 'jdbc:postgresql:///postgres?cloudSqlInstance=planningmeme:europe-west3:myinstance&socketFactory=com.google.cloud.sql.postgres.SocketFactory',
          outputFile: `gs://beam-beam/avroout/${folderName}/out`,
        },
        environment: {
          tempLocation: 'gs://beam-beam/temp',
          zone: "europe-west3-a"
        },
        jobName: 'PostgreSQL to Avro'
      }
    }, function(err, response) {
      if (err) {
        console.error("problem running dataflow template, error was: ", err);
      }
      console.log("Dataflow template response: ", response);
    });
  });
};

require('dotenv').config();

var fs  = require('fs');
var pg  = require('pg');
var aws = require('aws-sdk');

var region = process.env['AWS_REGION'];

if (!region || region === null || region === "") {
    region = "us-east-1";
    console.log("MC-ETL using default region " + region);
}

aws.config.update({region: region});

// Lambda function for invoking the ETL stored procecure.
exports.handler = function(event, context) {
  // Connect to the MC DBMS.
  var db = new pg.Client({
    user     : process.env.DB_USER,
    password : process.env.DB_PASS,
    database : process.env.DB_NAME,
    host     : process.env.DB_HOST,
    ssl: {
      ca: [fs.readFileSync('./assets/rds-combined-ca-bundle.pem')]
    }
  });

  db.connect(function(err){
    if ( err ) {
      console.log("ETL connect failed " + err.toString());
      context.fail(err);
    } else {
      console.log("ETL connect succeeded");

      // Insert into the ETL launch table to fire off trigger-based measures ETL.
      // We perform both MC-Granola and MC-JSON ETL here.

      var dataset_types = {
        granola: 0,
        mc_json: 1
      };

      var insert_query = 'insert into measures_etl_launch(dataset_type) values ' +
        (Object.keys(dataset_types).map(function(k) { return '(' + dataset_types[k] + ')'; }).join(', '));

      db.query(insert_query, function(err, result) {
        if ( err ) {
          console.log("ETL failed " + err);
          context.fail(err);
        } else {
          console.log("ETL succeeded");
          context.succeed();
        }
      });
    }
  });
}

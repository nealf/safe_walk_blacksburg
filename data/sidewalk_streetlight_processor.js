#! /usr/bin/env node

var pg = require('pg');

var conString = "postgres://postgres_admin:password@postgres1.ceipocejvkue.us-west-2.rds.amazonaws.com/blacksburg";

pg.connect(conString, function(err, client, done) {
  if(err) {
    return console.error('error fetching client from pool');
  }
  //Get a list of all the sidewalk GIDs
  client.query('SELECT DISTINCT gid FROM roads ORDER BY gid LIMIT 10;', function(err, roadResults) {
    if (err) {
      return console.error('Error fetching the initial roads');
    }
    console.log(roadResults.rows)
    processRoads(roadResults.rows);
  });
  done(); //release the client back to the pool
});


function processRoads(roadGIDs) {
  pg.connect(conString, function (err, client, done) {
    if (err) {
      return console.error('error fetching client from pool', err);
    }
    //Get all of the sidewalk segment points for each GID
    for (var i in roadGIDs) {
      client.query('SELECT gid, (ST_DumpPoints(geom)).path AS path, ST_AsGeoJSON((ST_DumpPoints(geom)).geom) AS geom FROM roads WHERE gid = $1;', [roadGIDs[i]["gid"],], function (err, roadPointResults) {
        if (err) {
          return console.error('error running query', err);
        }
        console.log('Number of road points: ', roadPointResults.rows.length);
        if (roadPointResults.rows.length == 0) {
          return;
        }

        var gid = roadPointResults.rows[0]['gid'];
        var sql = [];

        //This loops over each point in each road segment and creates a big SQL query array to find the closest sidewalk to each road point
        //Distance is in feet, and we'll set an arbitrary limit that it has to be within 25ft to associate it with a sidewalk
        for (var j = 0; j < roadPointResults.rows.length; j++) {
          var point = JSON.parse(roadPointResults.rows[j]['geom']);
          sql.push('(SELECT gid, ST_Distance(geom, ST_SetSRID(ST_MakePoint(' + point.coordinates[0] + ',' + point.coordinates[1] + '), 2284)) AS theDistance FROM sidewalks_2014 WHERE ST_Distance(geom, ST_SetSRID(ST_MakePoint(' + point.coordinates[0] + ',' + point.coordinates[1] + '), 2284)) <= 25.0 ORDER BY ST_Distance(geom, ST_SetSRID(ST_MakePoint(' + point.coordinates[0] + ',' + point.coordinates[1] + '), 2284)) LIMIT 1)');
        }
        processDistances(sql, gid);
      });
    }
    done();
  });
}

function processDistances(distanceQueryArray, roadGID) {
  pg.connect(conString, function (err, client, done) {
    if (err) {
      return console.error('error fetching client from pool', err);
    }
    client.query(distanceQueryArray.join(' UNION '), function (err, distanceResults) {
      if (err) {
        return console.error('Error running distance query', err);
      }
      //If over half the points from our UNION query returned null (ie sidewalk greater than distance limit), then it isn't associated with a sidewalk
      if (distanceResults.rows.length < (distanceQueryArray.length/2)) {
      	return console.log('Road not associated with any sidewalks');
      }
      //Otherwise we'll say it has a sidewalk
      updateRoadSegment(roadGID);
      
      /*var associatedRoads = [];
      for (var k = 0; k < distanceResults.rows.length; k++) {
        if (typeof associatedRoads[distanceResults.rows[k].gid] === 'undefined') {
          console.log(distanceResults.rows[k].gid);
          associatedRoads[distanceResults.rows[k].gid] = 0;
        } else {
          associatedRoads[distanceResults.rows[k].gid] += 1;
        }
      }
      //We should be done processing all of the points in this sidewalk segment, so let's go associate it with a road
      associateRoadSegment(associatedRoads);
      */
    });
    done();
  });
}

function updateRoadSegment(roadGID) {
  //Now we'll update the database, setting the hasSidewalks column = true for our GID that had sidewalk points associated with it  
  pg.connect(conString, function(err, client, done) {
    if(err) {
      return console.error('error fetching client from pool in func associate_road_segment', err);
    }
    client.query('UPDATE roads SET hasSidewalks = True WHERE gid = $1;', [roadGID,], function(err, result) {
      if(err) {
        return console.error('error setting hasSidewalks for gid=',roadGID, err);
      }
      console.log('Updated road GID: ', roadGID);
    });
    done();
  });
}


//Flip it around: Does every point on a road have a sidewalk within 8m? Any that don't but have a sidewalk associated with them should be flagged for review

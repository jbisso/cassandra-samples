var express = require('express');
var bodyParser = require('body-parser');
var cassandra = require('cassandra-driver');
var async = require('async');

// connect to a cluster one of whose nodes is running on localhost

var client = new cassandra.Client({ contactPoints : [ '127.0.0.1' ] });
client.connect(function(err, result) {
    console.log('Connected.');
});

// The app itself, mapping REST APIs to functions

var app = express();
app.use(bodyParser.json());
app.set('json spaces', 2);

// simple and prepared statements

var getSongs = 'SELECT * FROM simplex.songs;';
var getSongById = 'SELECT * FROM simplex.songs WHERE id = ?;';
var upsertSong = 'INSERT INTO simplex.songs (id, title, album, artist, tags, data)  '
    + 'VALUES(?, ?, ?, ?, ?, ?);';
var updateSongTags = "UPDATE simplex.songs SET tags = tags + ? WHERE id = ?;";
var deleteSongById = 'DELETE FROM simplex.songs WHERE id = ?;';

var getPlaylistById = 'SELECT * FROM simplex.playlists WHERE id = ?;';
var upsertPlaylist = 'INSERT INTO simplex.playlists (id, song_id, title, album, artist) '
    + 'VALUES(?, ?, ?, ?, ?);';

// REST API

app.get('/metadata', function(req, res) {
    res.json(client.hosts.slice(0).map(function (h) {
        return { address : h.address, rack : h.rack, datacenter : h.datacenter }
    }));
});

// schema creation and deletion

app.post('/keyspace', function(req, res) {
    client.execute("CREATE KEYSPACE IF NOT EXISTS simplex WITH replication " + 
                   "= {'class' : 'SimpleStrategy', 'replication_factor' : 3};",
                   afterExecution('Error: ', 'Keyspace created.', res));
});

app.post('/tables', function(req, res) {
    async.parallel([
        function(next) {
            client.execute('CREATE TABLE IF NOT EXISTS simplex.songs (' +
                'id uuid PRIMARY KEY,' +
                'title text,' +
                'album text,' +
                'artist text,' +
                'tags set<text>,' +
                'data blob' +
                ');',
                next);
        },
        function(next) {
            client.execute('CREATE TABLE IF NOT EXISTS simplex.playlists (' +
                'id uuid,' +
                'title text,' +
                'album text,' +
                'artist text,' +
                'song_id uuid,' +
                'PRIMARY KEY (id, title, album, artist)' +
                ');',
                next);
        }
    ], afterExecution('Error: ', 'Tables created.' , res));
});

app.delete('/keyspace/:name', function(req, res) {
    client.execute('DROP KEYSPACE ' + req.params.name + ';', 
        afterExecution('Error: ', 'Keyspace ' + req.params.name + ' dropped.', res));
});

// songs

app.get('/songs',  function(req, res) {
    client.execute(getSongs, null, function(err, result) {
        if (err) {
            res.status(404).send({ msg : 'Songs not found.' });
        } else {
            res.json(result);
        }
    });
});

app.get('/song/:id', function(req, res) {
    client.execute(getSongById, [ req.params.id ], function(err, result) {
        if (err) {
            res.status(404).send({ msg : 'Song not found.' });
        } else {
            res.json(result);        }
    });
});

app.post('/song', function(req, res) {
    var id = null;
    if ( ! req.body.hasOwnProperty('id')) {
        id = cassandra.types.uuid();
    } else {
        id = req.body.id;
    }
    client.execute(upsertSong,
        [id, req.body.title, req.body.album, req.body.artist, req.body.tags, null],
        afterExecution('Error: ', 'Song ' + req.body.title + ' upserted.', res));
});

app.post('/song/tags', function(req, res) {    
    client.execute(updateSongTags, [req.body.tags, req.body.id], 
        afterExecution('Error: ', 'Song ' + req.pody.id + ' tags updated.', res));
});

app.delete('/song/:id', function(req, res) {
    client.execute(deleteSongById, [ req.params.id ],
        afterExecution('Error: ', 'Song ' + id + ' deleted.', res));
});

// playlists

app.get('/playlist/:id', function(req, res) {
    var song = null;
    client.execute(getPlaylistById, [ req.params.id ], function(err, result) {
        if (err) {
            res.status(404).send({ msg : 'Playlist not found.' });
        } else {
            res.json(result);
            console.log('Playlist found.');
        }
    });
});

app.post('/playlist', function(req, res) {
    var id = null;
    if ( ! req.body.hasOwnProperty('id')) {
        id = cassandra.types.uuid();
    }
    client.execute(upsertPlaylist, [req.body.id, req.body.song_id, req.body.title, req.body.album, req.body.artist], 
        afterExecution('Error: ', 'Playlist ' + req.body.id + ' upserted.', res));
});

app.post('/shutdown', function(req, res) {
    client.shutdown(function(err, result) {
        if (err) {
            res.status(500).send({ msg : 'Cannot shut down client.' });
        } else {
        }
    });
});

// the server

var server = app.listen(3000, function() {
    console.log('Listening on port %d', server.address().port);
});

// callback function

function afterExecution(errorMessage, successMessage , res) {
    return function(err) {
        if (err) {
            return res.json(errorMessage + err);
        } else {
            res.json({ msg : successMessage });
        }
    };
}

/*

curl -H "Content-Type: application/json" -X GET http://localhost:3000/metadata

curl -H "Content-Type: application/json" -X POST http://localhost:3000/keyspace
curl -H "Content-Type: application/json" -X POST http://localhost:3000/tables

curl -H "Content-Type: application/json" -X POST --data @song001.json http://localhost:3000/song
curl -H "Content-Type: application/json" -X POST --data @song002.json http://localhost:3000/song
curl -H "Content-Type: application/json" -X POST --data @song003.json http://localhost:3000/song
curl -H "Content-Type: application/json" -X POST --data @song004.json http://localhost:3000/song

curl -H "Content-Type: application/json" -X POST --data @playlist001.json http://localhost:3000/playlist
curl -H "Content-Type: application/json" -X POST --data @playlist002.json http://localhost:3000/playlist
curl -H "Content-Type: application/json" -X POST --data @playlist003.json http://localhost:3000/playlist

curl -H "Content-Type: application/json" -X GET http://localhost:3000/song/756716f7-2e54-4715-9f00-91dcbea6cf50
curl -H "Content-Type: application/json" -X GET http://localhost:3000/songs

curl -H "Content-Type: application/json" -X DELETE http://localhost:3000/song/756716f7-2e54-4715-9f00-91dcbea6cf50
curl -H "Content-Type: application/json" -X DELETE http://localhost:3000/keyspace/simplex
*/


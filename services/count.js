const config = require('config');
const indexName = config.get('elasticsearch.index_name');

exports.count = (client, from, to, callback) => {
    client.count({
        index: indexName,
        body : {
            query: {
                range: {
                    timestamp : {
                        gte : from,
                        lt : to
                    }
                }
        }}
    }).then(function(resp) {
        console.log("Successful query!");
        console.log(JSON.stringify(resp, null, 4));
        callback({
            count: resp.body.count
        })
    }, function(err) {
        console.trace(err.message);
    });


}

exports.countAround = (client, lat, lon, radius, callback) => {
    // TODO Compter le nombre d'anomalies autour d'un point géographique, dans un rayon donné
    callback({
        count: 0
    })
}
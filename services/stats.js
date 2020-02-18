const config = require('config');
const indexName = config.get('elasticsearch.index_name');

exports.statsByArrondissement = (client, callback) => {
    client.search({
        index: indexName,
        body: {
            "aggs": {
                "arrondissements": {
                    "terms": {"field": "arrondissement.keyword", "size": 20}
                }
            }
        }
    }).then(function(resp) {
        console.log("Successful query!");
        console.log(JSON.stringify(resp, null, 4));

        callback(formatArrResponse(resp.body.aggregations.arrondissements.buckets));

        callback([]);
    }, function(err) {
        console.trace(err.message);
    });


}

exports.statsByType = (client, callback) => {
    client.search({
        index: indexName,
        body: { "aggs" : {
                "types": {
                    "terms": {"field": "type.keyword", "size": 5},
                    "aggs": {
                        "sous_types": {
                            "terms": {"field": "sous_type.keyword", "size": 5}
                        }
                    }
                }
            }
        }
    }).then(function(resp) {
        console.log("Successful query!");
        console.log(JSON.stringify(resp, null, 4));

        callback(formatTypeResponse(resp.body));

    }, function(err) {
        console.trace(err.message);
    });
}

exports.statsByMonth = (client, callback) => {
    // TODO Trouver le top 10 des mois avec le plus d'anomalies
    callback([]);
}

exports.statsPropreteByArrondissement = (client, callback) => {
    // TODO Trouver le top 3 des arrondissements avec le plus d'anomalies concernant la propreté
    callback([]);
}

function formatArrResponse(data) {
    let acc = [];
    for (let index = 0; index < data.length; index++) {
        const arrStat = data[index];
        acc.push({
            arrondissements: arrStat.key,
            count: arrStat.doc_count
        })
    }
    return acc;
}

function formatTypeResponse(data) {
    let buckets = data.aggregations.types.buckets;
    let results = buckets.map((type) => {
        return {
            type: type.key,
            count: type.doc_count,
            sous_types: type.sous_types.buckets.map((sous_type) => {
                return {
                    sous_type: sous_type.key,
                    count: sous_type.doc_count
                }
            })
        }
    });
    return results
}

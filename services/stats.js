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
    client.search({
        index: indexName,
        body: {
            aggs: {
                all_months: {
                    date_histogram: {
                        field: "timestamp",
                        calendar_interval: "1M"
                    }
                }
            }
        }
    }).then(function(resp) {
        console.log("Successful query!");
        console.log(JSON.stringify(resp, null, 4));

        callback(formatMonthResponse(resp.body));

    }, function(err) {
        console.trace(err.message);
    });
}

exports.statsPropreteByArrondissement = (client, callback) => {
    client.search({
        index: indexName,
        body: {
            query: {
                bool: {
                    must: [
                        {
                            match: {type: "propreté"}
                        }
                    ]
                }
            },
            aggs: {
                arrondissements : {
                    terms: {
                        field: "arrondissement.keyword", size: 3
                    }
                }
            }
        }
    }).then(function(resp) {
        console.log("Successful query!");
        console.log(JSON.stringify(resp, null, 4));
        callback(formatTypeArrResponse(resp.body));

    }, function(err) {
        console.trace(err.message);
    });
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

function formatTypeArrResponse(data) {
    let buckets = data.aggregations.arrondissements.buckets;
    let results = buckets.map((bucket) => {
        return {"arrondissement": bucket.key, "count": bucket.doc_count}
    });
    return results;
}

function formatMonthResponse(data) {
    let buckets = data.aggregations.all_months.buckets;
    buckets = buckets.sort((m1, m2) => {
        return m2.doc_count - m1.doc_count;
    });
    let top10Months = buckets.slice(0,10);
    return top10Months.map((month) => {
        let dateObj = new Date(month.key_as_string);
        let mth = dateObj.getMonth() + 1;
        let dateStr = ((mth < 10) ? "0" + mth : mth)+ "/" + dateObj.getFullYear();
        return { month: dateStr, count: month.doc_count}
    })
}

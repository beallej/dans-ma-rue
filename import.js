const config = require('config');
const csv = require('csv-parser');
const fs = require('fs');
const { Client } = require('@elastic/elasticsearch');
const indexName = config.get('elasticsearch.index_name');

async function run () {
    // Create Elasticsearch client
    const client = new Client({ node: config.get('elasticsearch.uri') });

    // Création de l'indice
    client.indices.create({ index: indexName }, (err, resp) => {
        if (err) console.trace(err.message);
    });


    let anomalies = [];
    // Read CSV file
    const BULK_SIZE = 20000;
    fs.createReadStream('dataset/dans-ma-rue.csv')
        .pipe(csv({
            separator: ';'
        }))
        .on('data', (data) => {
            anomalies.push({
                object_id: data["OBJECTID"],
                annee_declaration: data["ANNEE DECLARATION"],
                mois_delcaration: data["MOIS DECLARATION"],
                type: data["TYPE"],
                sous_type: data["SOUSTYPE"],
                code_postal: data["CODE_POSTAL"],
                ville:data["VILLE"],
                arrondissement: data["ARRONDISSEMENT"],
                prefixe: data["PREFIXE"],
                interventant: data["INTERVENANT"],
                conseil_de_quartier: data["CONSEIL DE QUARTIER"],
                location: data["geo_point_2d"]
            });
            if (anomalies.length >= BULK_SIZE) {
                let anomalies_full = anomalies;
                anomalies = [];
                client.bulk(createBulkInsertQuery(anomalies_full), (err, resp) => {
                    if (err) console.error(err);
                    else console.log(`Inserted ${resp.body.items.length} events`);
                });
            }
            console.log(data);
        })
        .on('end', () => {

            client.bulk(createBulkInsertQuery(anomalies), (err, resp) => {
                if (err) console.error(err);
                else console.log(`Inserted ${resp.body.items.length} events`);
                client.close();
                console.log('Terminated!');
            });
        });
}

// Fonction utilitaire permettant de formatter les données pour l'insertion "bulk" dans elastic
function createBulkInsertQuery(anomalies) {
    const body = anomalies.reduce((acc, anomalie) => {
        const { annee_declaration, mois_declaration,
            type, sous_type, code_postal, ville, arrondissement,
            prefixe, intervenant, conseil_de_quartier, location } = anomalie;
        acc.push({ index: { _index: indexName, _type: '_doc', _id: anomalie.object_id } })
        acc.push({ annee_declaration, mois_declaration,
            type, sous_type, code_postal, ville, arrondissement,
            prefixe, intervenant, conseil_de_quartier, location })
        return acc
    }, []);

    return { body };
}

run().catch(console.error);

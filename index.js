const express = require('express')
const app = express()
const port = process.env.PORT || 3500
const AccessToken = process.env["ACCESS_LIBRARY_TOKEN"];

var rp = require("request-promise")
const csv = require('csvtojson')

app.locals.env = process.env

require("./data/db")()
const Politicians = require("./data/Politicians")
const { request } = require('express')

app.get('/sync/betim', (req, res) => {
    console.log('Request was made')
    
    csv().fromFile('./consulta_cand_2020/consulta_cand_2020_MG.csv').then((json) => {
        var betimPoliticians = [];
        json.forEach(politician => {
            var objectKeys = Object.keys(politician)[0].split(';')
            var values = politician[Object.keys(politician)[0]].split(';')
            var object = {};
            var count = 0;
            objectKeys.forEach(key => {
                object[key.replace("\"", "").replace("\"", "")] = values[count].replace("\"", "").replace("\"", "")
                count++;
            })
            if (object.DS_DETALHE_SITUACAO_CAND === 'DEFERIDO' && object.NM_UE === 'BETIM') {
                betimPoliticians.push({
                    politics_cpf: object.NR_CPF_CANDIDATO,
                    politics_data: object,
                    personal_data: {}
                })
            }
        })
        savePoliticians(betimPoliticians, res);
        console.log(betimPoliticians);   
    })
})

app.get('/list/betim', (req, res) => {
    Politicians.find().then(result => {
        res.json(
            {size: result.length,
                data: result});
    })
});

app.get('/list/betim/cools', (req, res) => {
    Politicians.find().then(result => {
        var filteredResult = result.filter(politician => politician.politics_data.ST_DECLARAR_BENS === `S` 
        && politician.personal_data.Result[0].BasicData.TaxIdStatus === `REGULAR`
        && politician.nada_consta.Result[0].OnlineCertificates[0].BaseStatus === `NADA CONSTA`
        && politician.politics_data.SG_PARTIDO !== `PSL`
        && politician.politics_data.SG_PARTIDO !== `PP`
        && politician.politics_data.SG_PARTIDO !== `PMDB`
        && politician.politics_data.SG_PARTIDO !== `PT`
        && politician.politics_data.SG_PARTIDO !== `PSDB`
        && politician.politics_data.SG_PARTIDO !== `DEM`
        && politician.politics_data.SG_PARTIDO !== `PL`
        && politician.politics_data.SG_PARTIDO !== `PTC`
        && politician.politics_data.DS_GENERO === `FEMININO`
        && politician.politics_data.DS_COR_RACA !== `BRANCA`
        && politician.personal_data.Result[0].BasicData.Age < 40)
        res.json(
            {size: filteredResult.length,
                data: filteredResult});
    })
});

app.get('/sync/betim/personaldata', (req, res) => {
    Politicians.find().then(result => {
        Promise.all(result.map(politician => processPersonalData(politician))).then(result => {
            res.send("all data processed")
        })
    })
});

async function processPersonalData(politician) {
    console.log(`Politician to load: ${politician.politics_cpf}` )
    var options = {
        method: 'POST',
        uri: 'https://bigboost.bigdatacorp.com.br/peoplev2',
        body: {
            Datasets: "basic_data",
            "q": `doc{${politician.politics_cpf}}`,
            AccessToken
        },
        json: true // Automatically stringifies the body to JSON
    };
    let result = await rp(options)
    console.log(`Politician loaded: ${politician.politics_cpf}`)
    politician.personal_data = result;
    var saveResult = await new Politicians(politician).save();

}

app.get('/sync/betim/nadaconsta', (req, res) => {
    Politicians.find().then(result => {
        Promise.all(result.map(politician => processNadaConsta(politician))).then(result => {
            res.send("all data processed")
        })
    })
});

async function processNadaConsta(politician) {
    console.log(`Politician to load: ${politician.politics_cpf}` )
    var options = {
        method: 'POST',
        uri: 'https://bigboost.bigdatacorp.com.br/peoplev2',
        body: {
            Datasets: "ondemand_nada_consta",
            "q": `doc{${politician.politics_cpf}}`,
            AccessToken
        },
        json: true // Automatically stringifies the body to JSON
    };
    let result = await rp(options)
    console.log(`Politician loaded: ${politician.politics_cpf}`)
    politician.nada_consta = result;
    var saveResult = await new Politicians(politician).save();

}

app.get('/sync/betim/pf', (req, res) => {
    Politicians.find().then(result => {
        Promise.all(result.map(politician => processPf(politician))).then(result => {
            res.send("all data processed")
        })
    })
});

async function processPf(politician) {
    console.log(`Politician to load: ${politician.politics_cpf}` )
    var options = {
        method: 'POST',
        uri: 'https://bigboost.bigdatacorp.com.br/peoplev2',
        body: {
            Datasets: "ondemand_pf_antecedente",
            "q": `doc{${politician.politics_cpf}}`,
            AccessToken
        },
        json: true // Automatically stringifies the body to JSON
    };
    let result = await rp(options)
    console.log(`Politician loaded: ${politician.politics_cpf}`)
    politician.policia_federal = result;
    var saveResult = await new Politicians(politician).save();

}

async function savePoliticians(betimPoliticians, res){
    var result = await Promise.all(betimPoliticians.map(polMap => savePolitician(polMap)));
    res.json({betimSaved: true})
}

app.get('/sync/betim/pc', (req, res) => {
    Politicians.find().then(result => {
        Promise.all(result.map(politician => processPc(politician))).then(result => {
            res.send("all data processed")
        })
    })
});

async function processPc(politician) {
    console.log(`Politician to load: ${politician.politics_cpf}` )
    var options = {
        method: 'POST',
        uri: 'https://bigboost.bigdatacorp.com.br/peoplev2',
        body: {
            Datasets: "ondemand_pc_antecedente_by_state",
            "q": `doc{${politician.politics_cpf}},uf(MG)`,
            AccessToken
        },
        json: true // Automatically stringifies the body to JSON
    };
    let result = await rp(options)
    console.log(`Politician loaded: ${politician.politics_cpf}`)
    politician.policia_civil = result;
    var saveResult = await new Politicians(politician).save();

}

async function savePoliticians(betimPoliticians, res){
    var result = await Promise.all(betimPoliticians.map(polMap => savePolitician(polMap)));
    res.json({betimSaved: true})
}

async function savePolitician(politicianToSave){
    let politician = await Politicians.findOne({politics_cpf: politicianToSave.politics_cpf});
    if (politician) {
        politician.politics_data = politicianToSave.politics_data;
        await new Politicians(politician).save();
    
    } else {
        var result = await new Politicians(politicianToSave).save();
    }
    
}

app.listen(port, () => {
    console.log(`Brasil conciente está rodando em http://localhost:${port}`)
})

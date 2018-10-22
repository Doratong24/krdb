var rest = require('restler');
var ms = require('ms');
var sc = require('./status_code');

var kairos_prt = '32222';
var kairos_url = 'http://nxn1.kube.nexpie.com:' +
                    kairos_prt + '/';
var kairos_usr = 'tsdbadmin';
var kairos_pwd = 'RzuCqG14NrWXMfbj*s6m8v';

var input = {
    key: 'bd2w9fkc-bobclient',
    ts: 1539939867893,
    pts:
        [{
            value: 6,
            attr: 'location.x',
            module: { feed: { gap: '50s', duration: '1d' } }
        },
        {
            value: 29.1,
            attr: 'temp',
            module: { feed: { gap: '10s', duration: '1d' } }
        },
        {
            value: 73.8,
            attr: 'humid',
            module: { feed: { gap: '10s', duration: '1d' } }
        }]
}

var q_output = {
    start_absolute: 1539939867893 - 3600000,
    end_absoulte: new Date().getTime(),
    metrics: [{
        name: "bd2w9fkc-bobclient",
        tags: {
            "attr": 'temp'
        }
    }]
}

// push
function feed (data) {
    var device_id = data.key;
    var timestamp = data.ts;

    var f_data = [];
    var eachdata;

    for (var i = 0; i < data.pts.length; i++) {
        eachdata = {
            name:       device_id,
            timestamp:  timestamp,
            value:      data.pts[i].value,
            ttl:        ms(data.pts[0].module.feed.duration),
            tags: {
                "attr": data.pts[i].attr,
                "gap":  ms(data.pts[0].module.feed.gap),
                "raw":  data.pts[i].value,
                "hrs":  0,
                "day":  0,
                "wks":  0,
                "mth":  0,
                "yrs":  0
            }
        };
        f_data.push(eachdata);
    }

    rest.post(kairos_url + 'api/v1/datapoints', {
        username: kairos_usr,
        password: kairos_pwd,
        timeout: 5000,
        data: JSON.stringify(f_data)
    }).on('timeout', function(ms) {
        console.log('not response in ' + ms + ' ms');
    }).on('complete', function(data, response) {
        console.log('post status code| ' + sc.http_codeToStatus(response.statusCode));
    })
}

// query 
function get (q_data) {
    rest.post(kairos_url + 'api/v1/datapoints/query', {
        username: kairos_usr,
        password: kairos_pwd,
        timeout: 5000,
        data: JSON.stringify(q_data)
    }).on('timeout', function (ms) {
        console.log('not response in ' + ms + ' ms');
    }).on('complete', function (qres, response) {
        console.log('status code (qres): ' +
            sc.http_codeToStatus(response.statusCode));
        console.dir(qres.queries[0].results[0].values);
    });
}

feed(input);
get(q_output);
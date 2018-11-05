var rest = require('restler');
var ms = require('ms');
var sc = require('./status_code');
var plotly = require('plotly')('Doratong24', 'mr63tjr4ga');

var kairos_usr = 'tsdbadmin';
var kairos_pwd = 'RzuCqG14NrWXMfbj*s6m8v';
var kairos_prt = '32222';
var kairos_url = 'http://' + kairos_usr + ":" + kairos_pwd +
    '@nxn1.kube.nexpie.com:' +
    kairos_prt + '/';

var input = {
    key: '__testdevice',
    ts: new Date().getTime(),
    pts:
        [{
            value: 225.06,
            attr: 'power.Voltage',
            type: 'number',
            arg: { gap: '30s', duration: '7d' }
        }, {
            value: 0,
            attr: 'power.Power',
            type: 'number',
            arg: { gap: '30s', duration: '7d' }
        }]
}

/**
 * Push data from query into KairosDB
 * @param {object} data - An input data which must include key, ts, and pts
                    *      - key: device id
                    *      - ts:  timestamp of incoming data value
                    *      - pts: an array of incoimg data from device which includes:
                    *           - value: value of device
                    *           - attr:  a value type/ column name
                    *           - type:  type of input value
                    *           - arg:   include two property which are:
                    *               - gap: least gap time between each input from device
                    *               - duration: a time-to-live of this data value
 * @param {object} tag 
 */
function feed(data, tag) {
    var device_id = data.key;
    var timestamp = data.ts;

    var f_data = [];
    var eachdata;

    var taginput;
    var randval = Math.random();

    for (var i = 0; i < data.pts.length; i++) {
        taginput = tag ? tag : {
            "attr": data.pts[i].attr,
            "gap": data.pts[0].arg.gap,
            "range_tag": "raw"
        }
        eachdata = {
            name: device_id,
            datapoints: [[timestamp,
                data.pts[i].value + randval]],
            ttl: (data.pts[0].arg ?
                (data.pts[0].arg.duration ?
                    ms(data.pts[i].arg.duration) :
                    ms('5m'))
                : ms('5min')) / 1000,
            tags: taginput
        };
        f_data.push(eachdata);
        console.log(eachdata);
    }

    rest.post(kairos_url + 'api/v1/datapoints', {
        timeout: 5000,
        data: JSON.stringify(f_data)
    }).on('timeout', function (ms) {
        console.log('not response in ' + ms + ' ms');
    }).on('complete', function (data, response) {
        console.log('status code | ' +
            sc.http_codeToStatus(response.statusCode));
    })
}

/**
 * Decrease a range of timeframe for one step back
 * to make a summary of an updated data query
 * @param {string} range - a timeframe range
 */
function decrease_range_for_shown(range) {
    if (range == '1h') return 'raw';
    else if (range == '1d') return 'raw';
    else if (range == '1w') return '1h';
    else if (range == '1m') return '1h';
    else if (range == '1y') return '1d';
    else return 'raw';
}

/**
 * Decrease a range of timeframe for one step back
 * to make a summary of an updated data query
 * @param {string} range - a timeframe range
 */
function decrease_range_for_average(range) {
    if (range == '1h') return 'raw';
    else if (range == '1d') return '1h';
    else if (range == '1w') return '1d';
    else if (range == '1m') return '1w';
    else if (range == '1y') return '1m';
    else return 'raw';
}

/**
 * To check if the range is validate or not
 * since 1m in ms package means 1 minute, not one month
 * So it should convert 1m to 4w (4 weeks) instead to represent one month
 * @param {string} range - a value for validatation
 */
function validateRange(range) {
    var valid = ['raw', '1h', '1d', '1w', '1m', '1y'];
    if (valid.indexOf(range) == -1) return 'raw';
    else if (range == '1m') return '4w';
    else return range;
}

/**
 * Query the data from KairosDB
 * @param {string} device_id - a device name
 * @param {string} range - the specify range for query the result, 
 *                      can be chosen between 'raw', '1d', '1w', '1m', and '1y'
 */
function get_query(device_id, range) {
    var now = new Date().getTime()
    var q_data = {
        start_absolute:
            now - ms(validateRange(range)),
        end_absoulte: now,
        metrics: [{
            group_by: [{
                name: "tag",
                tags: ["attr", "range_tag"]
            }],
            tags: { range_tag: decrease_range_for_shown(range) },
            name: device_id
        }]
    }

    console.log(q_data);

    rest.get(kairos_url +
        'api/v1/datapoints/query?query=' +
        JSON.stringify(q_data)
    ).on('complete', function (qres, response) {
        console.log('status code (qres): ' +
            sc.http_codeToStatus(response.statusCode));
        console.dir(qres.queries[0].results);
        var res = qres.queries[0].results;
        for (var i = 0; i < res.length; i++) {
            var t_graph = [], v_graph = [];
            var valtype = res[i].values;
            // console.log(valtype);
            for (var j = 0; j < valtype.length; j++) {
                var mstodate = new Date(valtype[j][0]);
                t_graph.push(mstodate.toISOString());
                v_graph.push(valtype[j][1]);
            }
            var plotdata = [{
                x: t_graph,
                y: v_graph,
                type: "scatter"
            }];
            var graphOptions = { filename: "date-axes", fileopt: "overwrite" };
            plotly.plot(plotdata, graphOptions, function (err, msg) {
                console.log(msg);
            });
            console.log(plotdata);
        }
    });
}

/**
 * Make a query for querying an update data
 * @param {string} device_id - a device name or id
 * @param {object} qres - a response from previous query
 * @param {string} range - range for search to query data
 */
function update_query(device_id, qres, range) {
    var res = qres.queries[0].results;

    for (var i = 0; i < res.length; i++) {
        // check if there is a value for an update or not
        if (res[i].values.length == 0) {
            console.log(
                "Error in '" + res[i].name + "': " +
                "No data available for an update!");
            continue;
        }
        // create new feed
        var newfeed = {
            key: device_id,
            ts: res[i].values[0][0],
            pts: [{
                value: res[i].values[0][1],
                attr: res[i].tags.attr[0],
                arg: {
                    gap: res[i].tags.gap[0],
                    duration: '7d'
                }
            }]
        };
        // create tag for new feed
        var newtag = {
            "attr": res[i].tags.attr[0],
            "gap": res[i].tags.gap[0],
            "range_tag": range
        }
        feed(newfeed, newtag);
    }
}

/**
 * Query data from the query object in use for updating data
 * @param {string} device_id - a device name or id
 * @param {object} q_data - query object
 * @param {string} range - range for search to query data
 */
function get_query_for_update(device_id, q_data, range) {
    rest.get(kairos_url +
        'api/v1/datapoints/query?query=' +
        JSON.stringify(q_data)
    ).on('complete', function (qres, response) {
        console.log('status code (qres): ' +
            sc.http_codeToStatus(response.statusCode));
        console.dir(qres.queries[0].results);

        if (qres.queries[0].results)
            update_query(device_id, qres, range);
        else
            console.log(
                "Metrics '" + device_id +
                "' is not available for an update");
    });
}

/**
 * Summary the data in database with a specific range of time
 * @param {string} device_id - a device name or id
 * @param {string} range - an updated range
 * @param {number} qtime - a time that need to query, must be the latest
 */
function summary(device_id, range, qtime) {
    var mls = ms(validateRange(range));

    var now = qtime ? qtime : new Date().getTime();
    var ave_metric = {
        metrics: [{
            tags: {
                range_tag: decrease_range_for_average(range)
            },
            name: device_id,
            group_by: [{
                "name": "tag",
                "tags": ["attr"]
            }],
            aggregators: [{
                name: "avg",
                sampling: {
                    value: mls,
                    unit: "milliseconds"
                }
            }]
        }],
        start_absolute: now - ms(range),
        end_absoulte: now,
    };
    console.log("update start.. " + mls + ' ms');
    get_query_for_update(device_id, ave_metric, range);
}

/**
 * For testing and debugging in sequence
 */
function run_all() {
    // feed(input);
    // get_query('__testdevice', '1d');
    summary('__testdevice', '1h');
}

run_all();
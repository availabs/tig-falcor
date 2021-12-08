const falcorGraph = require('./graph'),
    get = require('lodash.get');

jest.setTimeout(100000)

describe('tigDataSources By Length', () => {


    // test('tigDataSources Length', (done) => {
    //     //"10003", "10005", "10001", "11001", "12131", "12053"
    //     const getEvent = {
    //         'paths': [
    //             ['tig','datasources','length']
    //         ],
    //         'method':'get'
    //     }
    //     falcorGraph.respond({ queryStringParameters: getEvent }, (error, response) => {
    //         console.log('response',JSON.stringify(response))
    //         //expect(get(response, 'jsonGraph.capabilities.byId.1.name.value', null)).toBe("Notify NYC");
    //         done();
    //     });
    // });
    //
    // test('tigDataSources ByIndex', (done) => {
    //     //"10003", "10005", "10001", "11001", "12131", "12053"
    //     const getEvent = {
    //         'paths': [
    //             ['tig','datasources','byIndex',[{from:0,to:2}]]
    //         ],
    //         'method':'get'
    //     }
    //     falcorGraph.respond({ queryStringParameters: getEvent }, (error, response) => {
    //         console.log('response',JSON.stringify(response))
    //         //expect(get(response, 'jsonGraph.capabilities.byId.1.name.value', null)).toBe("Notify NYC");
    //         done();
    //     });
    // });
    //
    // test('tigDataSources ById', (done) => {
    //     //"10003", "10005", "10001", "11001", "12131", "12053"
    //     const getEvent = {
    //         'paths': [
    //             ['tig','datasources','byId',['45','8','2'],['id',
    //                 'name',
    //                 'description',
    //                 'current_version',
    //                 'data_starts_at',
    //                 'data_ends_at',
    //                 'origin_url',
    //                 'user_id',
    //                 'rows_updated_at',
    //                 'rows_updated_by_id',
    //                 'topic_area',
    //                 'source_type']]
    //         ],
    //         'method':'get'
    //     }
    //     falcorGraph.respond({ queryStringParameters: getEvent }, (error, response) => {
    //         console.log('response',JSON.stringify(response))
    //         //expect(get(response, 'jsonGraph.capabilities.byId.1.name.value', null)).toBe("Notify NYC");
    //         done();
    //     });
    // });

    // test('tigDataSources Views By Length', (done) => {
    //     //"10003", "10005", "10001", "11001", "12131", "12053"
    //     const getEvent = {
    //         'paths': [
    //             ['tig','datasources','views','sourceId',['5','71'],'length']
    //         ],
    //         'method':'get'
    //     }
    //     falcorGraph.respond({ queryStringParameters: getEvent }, (error, response) => {
    //         console.log('response',JSON.stringify(response))
    //         //expect(get(response, 'jsonGraph.capabilities.byId.1.name.value', null)).toBe("Notify NYC");
    //         done();
    //     });
    // });
    //
    // test('tigDataSources Views By Index', (done) => {
    //     //"10003", "10005", "10001", "11001", "12131", "12053"
    //     const getEvent = {
    //         'paths': [
    //             ['tig','datasources','views','sourceId',['5','71'],'byIndex',[{from:0,to:15}]]
    //         ],
    //         'method':'get'
    //     }
    //     falcorGraph.respond({ queryStringParameters: getEvent }, (error, response) => {
    //         console.log('response',JSON.stringify(response))
    //         //expect(get(response, 'jsonGraph.capabilities.byId.1.name.value', null)).toBe("Notify NYC");
    //         done();
    //     });
    // });
    //
    // test('tigDataSources Views By Id', (done) => {
    //     //"10003", "10005", "10001", "11001", "12131", "12053"
    //     const getEvent = {
    //         'paths': [
    //             ['tig','datasources','views','sourceId',['5','71'],'byId',['38','170'],[ 'id',
    //                 'name',
    //                 'description',
    //                 'source_id',
    //                 'current_version',
    //                 'data_starts_at',
    //                 'data_ends_at',]]
    //         ],
    //         'method':'get'
    //     }
    //     falcorGraph.respond({ queryStringParameters: getEvent }, (error, response) =>  {
    //         console.log('response',JSON.stringify(response))
    //         //expect(get(response, 'jsonGraph.capabilities.byId.1.name.value', null)).toBe("Notify NYC");
    //         done();
    //     });
    // });

    test('tigDataSources npmrds', (done) => {
        //"10003", "10005", "10001", "11001", "12131", "12053"
        const getEvent = {
            'paths': [
                ["tig","npmrds","1|2019",["COUNTY|36005","COUNTY|36059","COUNTY|36105","COUNTY|36087","COUNTY|36027","COUNTY|36085","COUNTY|36081","COUNTY|36119","COUNTY|36047","COUNTY|36079","COUNTY|36113","COUNTY|36061","COUNTY|36111","COUNTY|36103","COUNTY|36071","COUNTY|34019","COUNTY|34031","COUNTY|34025","COUNTY|34037","COUNTY|34023","COUNTY|34003","COUNTY|34027","COUNTY|34039","COUNTY|34035","COUNTY|34029","COUNTY|34017","COUNTY|34021","COUNTY|34013","COUNTY|09009","COUNTY|09005","COUNTY|09001"],"data"]
            ],
            'method':'get'
        }
        falcorGraph.respond({ queryStringParameters: getEvent }, (error, response) =>  {
            console.log('response',JSON.stringify(response))
            //expect(get(response, 'jsonGraph.capabilities.byId.1.name.value', null)).toBe("Notify NYC");
            done();
        });
    });
})

afterAll(() => {
    return falcorGraph.close();
});

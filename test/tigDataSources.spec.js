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

    test('tigDataSources Views By Length', (done) => {
        //"10003", "10005", "10001", "11001", "12131", "12053"
        const getEvent = {
            'paths': [
                ['tig','datasources','views','sourceId',['5','71'],'length']
            ],
            'method':'get'
        }
        falcorGraph.respond({ queryStringParameters: getEvent }, (error, response) => {
            console.log('response',JSON.stringify(response))
            //expect(get(response, 'jsonGraph.capabilities.byId.1.name.value', null)).toBe("Notify NYC");
            done();
        });
    });

    test('tigDataSources Views By Index', (done) => {
        //"10003", "10005", "10001", "11001", "12131", "12053"
        const getEvent = {
            'paths': [
                ['tig','datasources','views','sourceId',['5','71'],'byIndex',[{from:0,to:15}]]
            ],
            'method':'get'
        }
        falcorGraph.respond({ queryStringParameters: getEvent }, (error, response) => {
            console.log('response',JSON.stringify(response))
            //expect(get(response, 'jsonGraph.capabilities.byId.1.name.value', null)).toBe("Notify NYC");
            done();
        });
    });

    test('tigDataSources Views By Id', (done) => {
        //"10003", "10005", "10001", "11001", "12131", "12053"
        const getEvent = {
            'paths': [
                ['tig','datasources','views','sourceId',['5','71'],'byId',['38','170'],[ 'id',
                    'name',
                    'description',
                    'source_id',
                    'current_version',
                    'data_starts_at',
                    'data_ends_at',]]
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

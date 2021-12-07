var uuid = require('node-uuid'),
    aws = require('aws-sdk');
/*var database = require('./lib/models');
var OcdService = require("./lib/services/ocdService")*/
var request = require('request');
var gm = require('gm').subClass({imageMagick: true});
// var smartcrop = require('smartcrop-gm');
var Upload = require('s3-uploader');
module.exports.upload = function(req, res) {
    // console.log('</img/upload>', req.params, req.query, req.body)
    var filename = uuid.v4() + "_" + req.query.objectName;
    var mimeType = req.query.contentType;
    var fileKey = filename;
    var s3Options = {
        Bucket: 'img.electorate.io',
        region: 'us-east-1',
        path: 'upload/',
        acl: 'public-read',
        accessKeyId: 'AKIAIGYGO2HFNTV2RERQ',
        secretAccessKey: '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'
    }
    var s3 = new aws.S3(s3Options);
    var params = {
        Bucket: 'img.electorate.io',
        // Path: 'upload/',
        ACL: 'public-read',
        Key: 'upload/' + fileKey,
        Expires: 60,
        ContentType: mimeType,
    };
    s3.getSignedUrl('putObject', params, function(err, data) {
        if (err) {
            console.log(err);
            res.json({error: "Cannot create S3 signed URL"});
        }
        res.json({
            signedUrl: data,
            publicUrl: 'https://s3.amazonaws.com/img.electorate.io/upload/' + filename,
            filename: filename
        });
    });
}
function checkTrailingSlash(path) {
    if (path && path[path.length-1] !== '/') {
        path += '/';
    }
    return path;
}

module.exports.process = function(event, cb) {
    event = event.query
    processPerson(event.image, event.typeId, [event.width,event.height, event.x, event.y], function(data){
        cb.json(data)
    })
/*    database.init(function(orm){
        global.orm = orm;
        if(event.type === 'person') {
            processPerson(event.image, event.typeId, [event.width,event.height, event.x, event.y], function(data){
                cb(null, data)
            })
        }
    })*/
}
function processPerson (imgSrc, personId, dimensions, next) {
    fetchImage(imgSrc, function(img) {
        if (!img ) return next()
        applyManualCrop(img, personId + '.' + imgSrc.substr(imgSrc.length - 3), dimensions,  function(src){
            if(!src) return next()
            client.upload(src , {awsPath: 'official/' + personId + '/'}, function(err, versions, meta) {
                if (err) {
                    console.log('upload failed', err);
                    return next()
                }
                return next({publicUrl: versions[0].url})
                /*
                createImage(
                    imgSrc,
                    versions[0].url,
                    personId,
                    next
                )
                */
            });
        });
    });
}
/*
function createImage(src, dest, personId, next){
    var newImg = {
        orig: src,
        src: dest,
        type: 'profile',
        official: personId,
    }
    OcdService.create(orm, 'official_photo', newImg).then(function(official_photo){
        OcdService.update(orm, 'person',{id:personId},{profilePhoto: official_photo.id}).then(function(updatePerson){
            console.log('finished person', personId, 'photo', official_photo.id)
            next({
                personId,
                photo:  official_photo.src
            })
        });
    })
}
*/
function applyManualCrop(img, dest, dim, next) {
    gm(img)
        .crop(dim[0], dim[1], dim[2], dim[3])
        .resize(600, 600)
        .write(dest, function(error){
            if (error) {
                console.log('gm error', error);
                return next(null)
            }
            return next(dest)
        });
}
function fetchImage (src, next) {
    // console.log('fetch image', src, next)
    request(src, {encoding: null}, function process(error, response, body){
        if (error) {
            console.log('error:', error)
            next(null)
        }
        return next(body)
    });
}
//------------------------------
// function executeTasks(input) {
// 	var tasks = Array.prototype.concat.apply([], input);
// 	var task = tasks.shift();
//     task(function() {
//     		console.log('tasks left ',tasks.length)
//         if(tasks.length > 0)
//             executeTasks(tasks);
//     });
// }
// function processSync (imgSrc,personId) {
// 	return function(callback) {
// 		console.log('starting person', personId, imgSrc)
// 		processPerson (imgSrc, personId, callback)
// 	}
// }
// function applySmartCrop(img, dest, width, height, next) {
//   gm(img)
// 	.size(function (err, size) {
// 		if (err) {
//     	console.log('gm error', err);
//     	return next(null)
//     }
// 		smartcrop.crop(img, {width: width, height: height}).then(function(result) {
// 	    var crop = result.topCrop;
// 	    gm(img)
// 	      .crop(crop.width, crop.height, crop.x, crop.y)
// 	      .resize(width, height)
// 	      .write(dest, function(error){
// 	          if (error) {
// 	          	console.log('gm error', error);
// 	          	return next(null)
// 	          }
// 	          return next(dest)
// 	      });
// 	  });
// 	})
// }
var client = new Upload('img.electorate.io', {
    aws: {
        path: 'official/',
        region: 'us-east-1',
        acl: 'public-read',
        accessKeyId: 'AKIAIGYGO2HFNTV2RERQ',
        secretAccessKey: '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'
    },
    cleanup: {
        versions: false,
        original: false
    },
    original: {
        awsImageAcl: 'public-read'
    },
    versions: [
        {
            maxWidth: 1200
        }
        /*
        {
        maxWidth: 200,
        aspect: '1:1'
        },
        {
            maxHeight: 1200,
            maxWidth: 1200,
            format: 'jpg',
            suffix: '-l',
            quality: 80,
        },
        {
            maxWidth: 600,
            aspect: '1:1',
            suffix: '-m'
        },
        {
            maxWidth: 600,
            aspect: '1.91:1!h',
            suffix: '-mwide'
        },
        {
            maxHeight: 60,
            aspect: '1:1',
            format: 'png',
            suffix: '-th'
        }
        */
    ]
});

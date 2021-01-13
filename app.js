"use strict";

require("dotenv").config();
const fs = require('fs');
const Twitter = require('twitter');
const { Storage } = require('@google-cloud/storage');
const storage = new Storage();

var client = new Twitter({
  consumer_key: process.env.TWITTER_API_KEY,
  consumer_secret: process.env.TWITTER_API_SECRET_KEY,
  access_token_key: process.env.TWITTER_ACCESS_TOKEN,
  access_token_secret: process.env.TWITTER_ACCESS_TOKEN_SECRET
});

var keywords = 'tyson, food, arkansa';
var count = 0;
var tweetList = [];

async function uploadFile(tweetList) {
  var date = new Date();
  var bucketName = "twitter-gcp-bucket";
  var fileName = date.getTime() + ".json";
  var gcsFileName = "tweets/" + fileName;

  // Create Local File
  fs.writeFileSync(fileName, JSON.stringify(tweetList));

  // Upload Local File to Google Cloud Storage
  await storage.bucket(bucketName).upload(fileName, { destination: gcsFileName });
  console.log(`File uploaded to gs://${bucketName}/${gcsFileName}.`);

  // Delete Local File
  fs.unlinkSync(fileName);
}

client.stream('statuses/filter', { track: keywords, language: 'en' }, function (stream) {
  stream.on('data', function (event) {
    if (count == 100) {
      uploadFile(tweetList);
      count = 0;
      tweetList = [];
    }

    var jsonStr = { text: `${event.text}` };
    tweetList.push(jsonStr);
    count = count + 1;
  });

  stream.on('error', function (error) {
    console.log(error);
  });
});
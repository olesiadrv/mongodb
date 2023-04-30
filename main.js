const { MongoClient } = require('mongodb');
const https = require('https');
const fs = require('fs');
const zlib = require('zlib');

const url = 'https://popwatch-staging.s3.us-east-2.amazonaws.com/movies_1.gz';
const output = 'data.txt';
https.get(url, function(response) {
  response.pipe(zlib.createGunzip()).pipe(fs.createWriteStream(output)).on('finish', function() {

    console.log('The file is unzipped');
    const url = 'mongodb+srv://olesiavdarvai:QG1peSkq9wYjxjUQ@movie.c68ptt6.mongodb.net/test';
    const client = new MongoClient(url);
    const dbName = 'movie';
    async function main() {
      try {
        await client.connect();
        console.log('Connected successfully to server');
        const db = client.db(dbName);
        const collection = db.collection('documents');
        
        const lineReader = require('readline').createInterface({
          input: fs.createReadStream(output),
        });
        for await (const line of lineReader) {
          const data = JSON.parse(line);
          await collection.insertOne(data);
        }
         console.log('Data loaded to MongoDB');
        console.log('Data inserted');
      } catch (err) {
        console.error(err);
      } finally {
        await client.close();
      }
      return 'done';
    }
    main()
      .then(console.log)
      .catch(console.error);
  });
});

const mongodb = require('mongodb');
const async = require('async');

let chunkSize = parseInt(process.argv[2]);
if(!isFinite(chunkSize) || chunkSize <= 0) {
    console.log('Command-line argument should be a positive integer indicating processing chunk size.');
    process.exit(1);
}

const customerData = require('./m3-customer-data.json');
const addressData = require('./m3-customer-address-data.json');

const url = 'mongodb://localhost:27017/edx-course-db';
const collectionName = 'customer-data';

let totalRecords = customerData.length;
if(addressData.length !== totalRecords) {
    console.log('Data sizes must be equal.');
    process.exit(1);
}

// Take the ceiling of the division. Last chunk may be smaller if the 
// chunk size doesn't exactly divide into the data set size. 
let numChunks = Math.ceil(totalRecords / chunkSize);
console.log(`Will process ${numChunks} chunks of ${chunkSize} records each.`);

// A factory function that manufactures tasks to process a chunk of data
function createTask(chunkIndex, db) {
    let chunkStart = chunkIndex * chunkSize;
    let data1 = Array.prototype.slice.call(customerData, chunkStart, chunkStart + chunkSize);
    let data2 = Array.prototype.slice.call(addressData, chunkStart, chunkStart + chunkSize);
    return (() => {
        console.log(`Writing records ${chunkStart} to ${chunkStart + data1.length}.`);
        for(let i = 0; i < data1.length; i++) {
            let data = data1[i];
            Object.assign(data, data2[i]);
            db.collection(collectionName).insert(data, (error, results, next) => {
                if (error) return next(error);
            });
        }
    });
}

// Connect to the database
mongodb.connect(url, { useNewUrlParser: true }, (error, client) => {
    // Unlike what we were taught in the course, I have to dig one level into 
    // the object to find the thing we can use.
    let db = client.db('edx-course-db');

    // Clean out any old data from a previous run (probably wouldn't do this 
    // in production, but this is an excercise).
    db.dropCollection(collectionName, () => {
        console.log(`Starting with an empty collection: '${collectionName}'`);

        // Manufacture all the tasks to be parallelized
        let tasks = Array(numChunks).fill().map((item, index) => createTask(index, db));

        // Do all the tasks
        async.parallel(tasks, (error) => {
            if(error) return console.log(error);
            return client.close(false, (error) => {
                if(error) return console.log(error);
            });
        });
    });
});


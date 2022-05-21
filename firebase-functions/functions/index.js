const functions = require("firebase-functions");
const admin = require('firebase-admin');
const {getFirestore, FieldValue} = require("firebase-admin/firestore");
const fetch = require('node-fetch');
const cors = require('cors');

admin.initializeApp();

const db = getFirestore()
const colRef = db.collection('schools')
let base_url = "https://en.wikipedia.org/w/api.php";

// query Wikipedia API for a list of predefined schools, and update the DB
// switch to 'every 5 minutes' for debugging only
// exports.processSchoolUpdates = functions.pubsub.schedule('every 5 minutes').onRun(async context => {
exports.processSchoolUpdates = functions.pubsub.schedule('0 1 * * *').onRun(async context => {
    colRef.get()
    .then((snapshot) => {
        snapshot.forEach(doc => { 
            // process each document in that collection
            console.log('processing: ', doc.id, '=>', doc.data().name);

            let docRef = colRef.doc(doc.id);
            let school_name = doc.data().name;
            updateLangList(docRef, school_name);
            updatePageviews(docRef, school_name);
            // When the number of revisions is larger than 500, additional logic will be needed.
            updateRevisions(docRef, school_name);   
        });
    })
    .catch((err) => {
      console.log('Error while processing the documents', err);
    }); 
});

// retrieve the data for a specific school from firestore
exports.getSchool = functions.https.onRequest(async (req, res) => {
    let school_name = req.query.school_name;
    let resStr = "{";
    const snapshot = await colRef.get();
    snapshot.forEach(doc => {
        if (doc.data().name == school_name) {
            resStr += JSON.stringify("name") + ":" + JSON.stringify(doc.data().name) + ",";
            resStr += JSON.stringify("pageviews") + ":" + JSON.stringify(doc.data().pageviews) + ",";
            resStr += JSON.stringify("languages") + ":" + JSON.stringify(doc.data().languages);
        }
    });

    resStr += "}";
    console.log("=== getSchool function response: ", resStr);

    cors()(req, res, () => {
        res.send(resStr);
    });

    return;
});

// retrieve the school list from firestore
exports.getSchoolList = functions.https.onRequest(async (req, res) => {
    let resStr = "{" + JSON.stringify("school_list") + ":";
    let school_list = new Array();

    const snapshot = await colRef.get();
    snapshot.forEach(doc => {
        school_list.push(doc.data().name);
    });

    resStr += JSON.stringify(school_list) + "}";
    console.log("=== getSchoolList function response: ", resStr);

    cors()(req, res, () => {
        res.send(resStr);
    });

    return;
});

// retrieve the data for each school from firestore
exports.testFunction = functions.https.onRequest(async (req, res) => {
    let resStr = "{";

    const snapshot = await colRef.get();
    snapshot.forEach(doc => { 
        resStr += JSON.stringify(doc.id) + ":{";
        resStr += JSON.stringify("name") + ":" + JSON.stringify(doc.data().name) + ",";
        resStr += JSON.stringify("pageviews") + ":" + JSON.stringify(doc.data().pageviews) + ",";
        resStr += JSON.stringify("languages") + ":" + JSON.stringify(doc.data().languages);
        resStr += "}, "
    });

    resStr += "}";
    console.log("=== testFunction function response: ", resStr);

    cors()(req, res, () => {
        res.send(resStr)
    });

    return;
});

// ======

async function updateLangList(docRef, school_name) {   
    // query for 'langlinks'
    let params = {
        action: "query",
        titles: "",
        prop: "langlinks",
        format: "json"
    };  

    params.titles = school_name;
    let url = base_url + "?origin=*";
    Object.keys(params).forEach(function(key) {url += "&" + key + "=" + params[key];});

    let fetchResponse = await (await fetch(url)).json();

    let pages = fetchResponse.query.pages;
    let pageId = Object.keys(pages)[0];
    const langs = pages[pageId]["langlinks"];

    let cnt = 1;
    let langList = ['en'];
    for (let element of langs) {
        cnt++;
        langList.push(element["lang"]);
    }

    docRef.update({
        langCnt: cnt,
        languages: langList,
        updatedAt: FieldValue.serverTimestamp(),
    });
}

async function updatePageviews(docRef, school_name) { 
    // query for 'pageviews'
    let params = {
        action: "query",
        titles: "",
        prop: "pageviews",
        pvipdays: "60",
        formatversion: "2",
        format: "json"
    };

    params.titles = school_name;
    let url = base_url + "?origin=*";
    Object.keys(params).forEach(function(key) {url += "&" + key + "=" + params[key];});

    let fetchResponse = await (await fetch(url)).json();

    let pages = fetchResponse.query.pages;
    let pageId = Object.keys(pages)[0];
    const pageviews = pages[pageId]["pageviews"];

    let pageviews_per_week = new Map();

    for (const key of Object.keys(pageviews)) {
        let pv = parseInt(pageviews[key]);
        if (isNaN(pv)) { pv = 0; }
        let weekNumber = getWeekNumber(key);

        if (pageviews_per_week.has(weekNumber)) {
            pv = pageviews_per_week.get(weekNumber) + pv;
        }
        pageviews_per_week.set(weekNumber, pv);
    };

    let index = 0;
    for (let [key, value] of pageviews_per_week) {
        index += 1;
        // ignore the first pair, which might contain incomplete week pv.
        if (index == 1) continue;

        docRef.set({
            pageviews: {
                [key]: value,
            },
            updatedAt: FieldValue.serverTimestamp(),
        }, {merge: true});
    }
}

async function updateRevisions(docRef, school_name) {  
   // query for 'revisions'
   let params = {
        action: "query",
        titles: "",
        prop: "revisions",
        rvprop: "timestamp|user|comment",
        rvslots: "main",
        rvlimit: "500",
        rvstart: "",
        rvend: "2022-01-01T00:00:00Z",
        formatversion: "2",
        format: "json"
    };

    params.titles = school_name;
    let now = new Date();
    params.rvstart = now.toISOString().slice(0, 10) + "T00:00:00Z";

    let url = base_url + "?origin=*";
    Object.keys(params).forEach(function(key) {url += "&" + key + "=" + params[key];});

    let fetchResponse = await (await fetch(url)).json();

    let pages = fetchResponse.query.pages;
    let pageId = Object.keys(pages)[0];
    const revisions = pages[pageId]["revisions"];    
    
    let cnt = 0;
    let contributorList = new Set();
    for (let element of revisions) {
        cnt++;
        contributorList.add(element["user"]);
    }    

    docRef.update({
        revisionCnt: cnt,
        contributors: Array.from(contributorList),
        contributorCnt: contributorList.size,
        updatedAt: FieldValue.serverTimestamp(),
    });
}

function getWeekNumber(date) {
    let currentDate = new Date(date);
    let yearNumber = currentDate.getFullYear();
    let startDate = new Date(yearNumber, 0, 1);
    let days = Math.floor((currentDate - startDate) / (24 * 60 * 60 * 1000));
    let weekNumber = Math.ceil((startDate.getDay() + 1 + days) / 7);

    return yearNumber + "-" + weekNumber;
}
const util = require('util');

const { connDB } = require('./db/db_connection');
const conn2service = require('./db/conn2');

const fs = require('fs');
const XmlStream = require('xml-stream');

function uploadData() {
    const filePath = './myntra.xml';    // This file is the size of 1.5 GB and having the records arround 12 Lakh
                                        // I tried but due to github max upload size could not upload the same
                                        // file to repo if you are this you can take your own large file

    const readStream = fs.createReadStream(filePath);
    const xml = new XmlStream(readStream);

    console.log('xml read done');
    let cnt = 1;
    let data_set = [];
    xml.on('endElement: offer',  async (item) => {
        console.log('item', `${item.$.id} - ${cnt}` );
        // console.log(item);

        let available = 0;
        if (item.$.available) {
            available = 1;
        }

        data_set.push([item.$.id, available, item.categoryId, item.currencyId,
            item.custom_label_3, item.custom_label_4, item.description, item.manufacturer_info,
            item.mobile_ios_app_store_id, item.modified_time, item.name, item.oldprice,
            item.origin_country, item.picture, item.price, item.url, item.vendor,item.param]);

        if (cnt === 4000) {
            console.log('10000 records inserted');
            insertData(data_set);
            data_set = [];
            cnt = 1;
        }
        cnt++;
    });

    xml.on("end", function() {
        // writeStream.end();
        insertData(data_set);
        data_set = [];
        cnt = 1;
        console.log("finished");
    });
}

uploadData();

function insertData(data) {
    try {
        const conn2 = conn2service.createConnection();
        const queryDb = util.promisify(conn2.query).bind(conn2);
        queryDb("INSERT INTO products (product_id, is_available," +
            " category_id, currency_id, custom_label_3, custom_label_4, description," +
            " manufacturer_info, mobile_ios_app_store_id, modified_time, name," +
            " oldprice, origin_country, picture, price, url, vendor, param) VALUES ?", [data]);
        close_conn(conn2);
    } catch (e) {
        console.log('e', e);
    }
}

function close_conn(conn2) {
    conn2.end((err) => {});
}

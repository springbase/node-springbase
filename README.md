# Springbase Driver for NodeJS
## Plug-and-play relational data store for Node applications
<a href="http://springbase.com/node-driver">Homepage</a>

Quick and easy relational data for your NodeJS apps.

### Installation

    npm install springbase

### Quick example

    var springbase = require('springbase');
    var conn = new springbase.data.Connection({ 
      application: "1GmTdS2ayPts", 
      username: "<email-address>", 
      password: "<password>" 
    });
    var query = conn.getQuery("qryStoresByZipCode");
    query.on("ready", function() {
      var reader = query.execute({ zipCode: 94611 });
      reader.on("row", function(row) {
        console.log("Found store:", row);
      });
    });

### Insert into a table:

    var table = conn.getTable("Stores");
    table.on("ready", function() {
      table.insertRow({
        number: 115,
        zipCode: 94105,
        hours: "8 AM - 11 PM"
      });
    });

### Update table rows:

    // Change store hours for store with id 30
    table.updateRow(30, {
      hours: "8 AM - 11 PM"
    });// Change store hours for store with id 30

### Delete table rows:

    table.deleteRows(30);

### Read all rows from a table:

    var reader = table.openReader();
    reader.on("ready", function() {
      console.log("All stores:", reader.readAll());
    });

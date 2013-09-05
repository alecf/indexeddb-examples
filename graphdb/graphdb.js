// The Little GraphDB
//
// exposes a very simple graph store with two basic interfaces, add()
// and find() Stores graph as a bag of edges. An edge is a JavaScript
// object with three properties: source, property, and target.
//
// The actual objects that source, property, and target are arbitrary,
// though they are typically strings. One way of imagining this is as
// labeled edges in a traditional graph, so that an edge looks like:
//
//   source ------[property]----> target
//
// To create a graph, just give it a name::
//
//   var g = new GraphDB("celebs");
//
// Adding new edges is simple::
//
//   g.add({source: "Bob Dylan", "property": "born_in", "target": "Duluth"});
//   g.add({source: "Bob Dylan", "property": "born_on", "target": new Date("5/24/1941")});
//   g.add({source: "Duluth", "property": "contained_by", "target": "MN"});
//   g.add({source: "Steve Foucault", "property": "born_in", "target": "Duluth"});
//
// Then you can query who was born in Duluth::
//
//   g.find({"property": "born_in", "target": "duluth"}).onsuccess = function(e) {
//        var cursor = e.target.result;
//        if (!cursor) return;
//        cursor.continue();
//        console.log("Born in Duluth: ", cursor.value.source)
//   });
//
// Note that most APIs return IndexedDB objects, so the usual onsuccess/etc applies.
// (subject to change...)

function GraphDB(name) {
    var graphDB = this;

    graphDB.readyCallbacks = [];
    graphDB.ready = false;
    var keyPaths = graphDB.getKeyPaths();
    var firstKeyPath = keyPaths[keyPaths.length-1];

    graphDB.dbName = "graphDB-" + name;
    var req = indexedDB.open(graphDB.dbName, 4);
    req.onupgradeneeded = function(e) {
        var db = e.target.result;
        console.log("Creating GraphDB: " + graphDB.dbName, e.target.result);
        if (!('links' in db.objectStoreNames)) {
            var objectStore = db.createObjectStore('links', {keyPath: firstKeyPath});
            for (var i = 0; i < keyPaths.length; i++) {
                var keyPath = keyPaths[i];
                objectStore.createIndex(keyPath.join(','), keyPath);
            }
        }

    };

    req.onsuccess = function(e) {
        graphDB.db = e.target.result;
        graphDB.ready = true;
        graphDB.checkReady();
    };
}

GraphDB.prototype.checkReady = function() {
    if (!this.ready)
        return;
    var callback = this.readyCallbacks.pop();
    while (callback) {
        callback(this);
        callback = this.readyCallbacks.pop();
    }
}

GraphDB.prototype.whenReady = function(f) {
    this.readyCallbacks.push(f);
    this.checkReady();
};

GraphDB.INDEX_FIELDS = ["source", "property", "target"];
GraphDB.INDEX_PATHS = [["source"], ["property"], ["target"],
                   ["source", "property"], ["source", "target"], ["property", "target"],
                   ["source", "property", "target"]];

GraphDB.prototype.getKeyPaths = function() {
    // cheat for now
    return GraphDB.INDEX_PATHS;
};

GraphDB.prototype.getEdgeFields = function() {
    return GraphDB.INDEX_FIELDS;
};

GraphDB.combinations = function(list, len, ignore, indent) {
    if (list.length == 0 || list.length < len)
        return [];
    if (!indent) indent = 0;
    if (!ignore) ignore = 0;

    var indentStr = "";
    for (var i = 0; i< indent; i++) {
        indentStr += " ";
    }

    console.log(indentStr + ">>>", list);
    if (list.length == len) {
        console.log(indentStr+"<<<", list, " => ", [list]);
        return [list];
    }
    var result = [];
    for (i = 0; i < list.length; i++) {
        var sublist = list.slice(0,i).concat(list.slice(i+1));
        result = result.concat(combinations(sublist, len, ignore+1, indent+4).slice(ignore));
    }
    console.log(indentStr+"<<<", list, " => ", result);
    return result;
};

GraphDB.prototype.add = function(edge) {
    var graphDB = this;
    var transaction = this.db.transaction('links', 'readwrite');
    return transaction.objectStore('links').put(edge);
};

GraphDB.prototype.find = function(edgeTemplate) {
    if (!edgeTemplate)
        edgeTemplate = {};

    var graphDB = this;
    var transaction = graphDB.db.transaction('links');
    var query = graphDB.getIndexAndKey(edgeTemplate, transaction);
    return query.index.openCursor(query.key);
};

GraphDB.prototype.count = function(edgeTemplate) {
    if (!edgeTemplate)
        edgeTemplate = {};

    var graphDB = this;
    var transaction = graphDB.db.transaction('links');
    var query = graphDB.getIndexAndKey(edgeTemplate, transaction);
    return query.index.count(query.key);
};

GraphDB.prototype.groupby = function(properties, template) {
    var fakeTemplate = {};
    for (var i = 0; i < properties.length; i++) {
        fakeTemplate[properties[i]] = true;
    };

    var graphDB = this;
    var transaction = this.transaction('links');
    var query = graphDB.getIndexAndKey(fakeTemplate, transaction);
    var result = {};
    console.log("groupby opening cursor");
    var request = query.index.openKeyCursor();
    request.onsuccess = GraphDB.emit_unique(result);
    request.onerror = function(e) {
        console.log("Error opening cursor");
        result.onerror(e);
    };
    return result;
};

GraphDB.emit_unique = function(result) {
    var last_key = undefined;
    var count = 0;
    return function(e) {
        var cursor = e.target.result;
        if (cursor) {
            cursor.continue();

            if (last_key !== undefined &&
                !GraphDB.key_equals(last_key, cursor.key)) {
                result.onsuccess(last_key, count);
                count = 0;
            }
            else
                count++;
            last_key = cursor.key;
        } else {
            if (last_key !== undefined)
                result.onsuccess(last_key, count);
            result.onsuccess(null, 0);
            count++;
        }

    };
};


// shortcut to do a lookup of all values for a particular match - you
// get a single callback with just an array of values
GraphDB.prototype.lookupAll = function(edgeTemplate) {
    var resultRequest = {};
    var result = [];
    var req = this.find(edgeTemplate);
    req.onsuccess = function(e) {
        var cursor = e.target.result;
        if (cursor) {
            cursor.continue();
            result.push(cursor.value);
        }
        else if (resultRequest.onsuccess)
            return resultRequest.onsuccess(result);
    };
    req.onerror = function(e) {
        if (resultRequest.onerror)
            return resultRequest.onerror(e);
    };
    return resultRequest;
};

// returns an object with two fields:
// index: the index to do the lookup
// key: the key for that index
GraphDB.prototype.getIndexAndKey = function(edgeTemplate, transaction) {
    var presentFields = [];
    var fields = this.getEdgeFields();
    for (var i = 0; i < fields.length; i++) {
        var fieldName = fields[i];
        if (fieldName in edgeTemplate && edgeTemplate[fieldName] !== null)
            presentFields.push(fieldName);
    }
    var objectStore = transaction.objectStore('links');
    if (presentFields.length) {
        var key = [];
        for (i = 0; i < presentFields.length; i++)
            key.push(edgeTemplate[presentFields[i]]);

        var indexName = presentFields.join(',');

        var index = objectStore.index(indexName);
        return {key: key, index: index};
    }

    // no index, just return the main object store
    return {key: null, index: objectStore};
};

// hack stolen from
// http://stackoverflow.com/questions/1068834/object-comparison-in-javascript
GraphDB.key_equals = function(a,b)
{
  var p;
  for(p in a) {
      if(typeof(b[p])=='undefined') {return false;}
  }

  for(p in a) {
      if (a[p]) {
          switch(typeof(a[p])) {
              case 'object':
                  if (!a[p].equals(b[p])) { return false; } break;
              case 'function':
                  if (typeof(b[p])=='undefined' ||
                      (p != 'equals' && a[p].toString() != b[p].toString()))
                      return false;
                  break;
              default:
                  if (a[p] != b[p]) { return false; }
          }
      } else {
          if (b[p])
              return false;
      }
  }

  for(p in b) {
      if(typeof(a[p])=='undefined') {return false;}
  }

  return true;
};
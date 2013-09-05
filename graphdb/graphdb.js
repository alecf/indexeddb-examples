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

    graphDB.ready = false;
    var keyPaths = graphDB.getKeyPaths();
    var firstKeyPath = keyPaths[keyPaths.length-1];

    graphDB.dbName = "graphDB-" + name;
    var req = indexedDB.open(graphDB.dbName, 4);
    req.onupgradeneeded = function(e) {
        graphDB.db = e.target.result;
        console.log("Creating GraphDB: " + graphDB.dbName, e.target.result);
        if ('graph' in e.target.result.objectStoreNames) {
            console.log("Creating graph");
            graphDB.db.deleteObjectStore('graph');
        }
        if (!('graph' in graphDB.db.objectStoreNames)) {
            var objectStore = graphDB.db.createObjectStore('graph', {keyPath: firstKeyPath});
            for (var i = 0; i < keyPaths.length; i++) {
                var keyPath = keyPaths[i];
                objectStore.createIndex(keyPath.join(','), keyPath);
            }
        }

    };

    req.onsuccess = function(e) {
        graphDB.db = e.target.result;
        graphDB.ready = true;
        if (graphDB.readyCallback)
            graphDB.readyCallback();
    };
}

GraphDB.prototype.whenReady = function(f) {
    if (this.ready)
        f();
    else
        this.readyCallback = f;
};

// creates an entirely new transaction with the given mode
GraphDB.prototype.transaction = function(mode) {
    return this.graph.transaction(mode);
};

// Get the current transaction if it is still active. Note that this
// is a little funky, it assumes that lastRequest_ is always kept up
// to date. It would be nice if the current transaction could be
// queried for active-ness or not.
GraphDB.prototype.transaction_ = function() {
    // FIXME: right now everything is readwrite
    if (this.current_transaction_ &&
        this.lastRequest_ &&
        this.lastRequest_.readyState == "pending") {
        // console.log("Reusing transaction from:");
        // console.trace();

    } else {
        // console.log("Creating transaction from:");
        // console.trace();
        var graphDB = this;
        this.current_transaction_ = this.db.transaction("graph", "readwrite");
        var clear_all_timer = function() {
            // console.log("Clearing from a timer, ", graphDB.current_transaction_ ?
            //             " still around " : " already cleared");
                delete graphDB.lastRequest_;
                delete graphDB.current_transaction_;
            };
        var clear_all_oncomplete = function() {
            // console.log("Clearing from oncomplete, ", graphDB.current_transaction_ ?
            //             " still around " : " already cleared");
                delete graphDB.lastRequest_;
                delete graphDB.current_transaction_;
            };
        this.current_transaction_.oncomplete = clear_all_oncomplete;
        // would be nice not to need this!
        setTimeout(clear_all_timer, 1);

    }
    return this.current_transaction_;
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

GraphDB.prototype.add = function(edge, transaction) {
    var graphDB = this;
    var trans = transaction || this.transaction_();
    try {
        this.lastRequest_ = trans.objectStore("graph").put(edge);
        return this.lastRequest_;
    } catch(E) {
        console.log("error adding ", edge, ": ", E);
        delete this.lastRequest_;
        delete this.current_tansaction_;

        // try once again
        trans = transaction || this.transaction_();
        this.lastRequest_ = trans.objectStore("graph").put(edge);
        return this.lastRequest_;
        throw E;
    }
};

GraphDB.prototype.find = function(edgeTemplate, transaction) {
    if (!edgeTemplate)
        edgeTemplate = {};

    var graphDB = this;

    var query = graphDB.getIndexAndKey(edgeTemplate, transaction);
    try {
        this.lastRequest_ = query.index.openCursor(query.key);
    } catch (e) {
        delete this.lastRequest_;
        this.lastRequest_ = query.index.openCursor(query.key);
    }
    return this.lastRequest_;
};

GraphDB.prototype.find_intersect = function() {
    var requests = [];
    for (var i = 0; i < arguments.length; i++) {
        requests.push(this.find(arguments[i]));
    }

};

GraphDB.prototype.count = function(edgeTemplate, transaction) {
    if (!edgeTemplate)
        edgeTemplate = {};

    var graphDB = this;

    var query = graphDB.getIndexAndKey(edgeTemplate, transaction);
    this.lastRequest_ = query.index.count(query.key);
    return this.lastRequest_;
};

GraphDB.prototype.groupby = function(properties, template, transaction) {
    var fakeTemplate = {};
    for (var i = 0; i < properties.length; i++) {
        fakeTemplate[properties[i]] = true;
    };

    var graphDB = this;
    var query = graphDB.getIndexAndKey(fakeTemplate, transaction);
    var result = {};
    console.log("groupby opening cursor");
    this.lastRequest_ = query.index.openKeyCursor();
    this.lastRequest_.onsuccess = GraphDB.emit_unique(result);
    this.lastRequest_.onerror = function(e) {
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
    var trans = transaction || this.transaction_();
    var objectStore = trans.objectStore("graph");
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